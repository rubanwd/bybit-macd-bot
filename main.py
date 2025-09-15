# main.py
import os
import time
import logging
from dotenv import load_dotenv
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from bybit_api import BybitAPI
from indicators import macd, rsi, atr
from telegram_utils import TelegramClient
from reporter import build_report_txt, write_report_file
from utils import append_history


# ----- helpers & config -----

TF_TO_BYBIT = {
    "5M": "5",
    "15M": "15",
    "30M": "30",
    "1H": "60",
    "4H": "240",
    "6H": "360",
    "12H": "720",
    "1D": "D",
    "1W": "W",
    "1M": "M",
}

LONG_TF_CODES = {"W", "M"}  # для этих берём меньшее limit

def setup_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except Exception:
        return default

def parse_timeframes(env_val: str):
    if not env_val:
        return ["1D", "1W"]
    items = [x.strip().upper() for x in env_val.split(",") if x.strip()]
    valid = []
    for x in items:
        if x in TF_TO_BYBIT:
            valid.append(x)
        else:
            logging.warning(f"Игнорирую неизвестный таймфрейм в .env: {x}")
    # без фанатизма: минимум один
    return valid or ["1D", "1W"]

def check_sort_tf(sort_tf: str, timeframes):
    s = (sort_tf or "").strip().upper()
    if s in timeframes:
        return s
    # если неверный — берём первый из списка
    logging.warning(f"SORT_TF={sort_tf} не найден в TIMEFRAMES. Использую {timeframes[0]}.")
    return timeframes[0]

def kline_to_df(klines):
    return pd.DataFrame(klines)[["open", "high", "low", "close"]].astype(float)

def compute_indicators(df: pd.DataFrame, macd_fast, macd_slow, macd_signal, rsi_period, atr_period, need_atr: bool):
    macd_line, signal_line, hist = macd(df["close"], macd_fast, macd_slow, macd_signal)
    rsi_series = rsi(df["close"], rsi_period)
    atr_series = atr(df["high"], df["low"], df["close"], atr_period) if need_atr else None
    return macd_line, signal_line, hist, rsi_series, atr_series

def classify_trend(macd_line: pd.Series, signal_line: pd.Series, hist: pd.Series) -> str:
    bull = (macd_line.iloc[-1] > signal_line.iloc[-1]) and (hist.iloc[-1] > 0)
    bear = (macd_line.iloc[-1] < signal_line.iloc[-1]) and (hist.iloc[-1] < 0)
    if bull:
        return "BULL"
    if bear:
        return "BEAR"
    return "NEUTRAL"


# ----- main loop -----

def main_loop():
    load_dotenv()
    setup_logging()

    # === static ENV ===
    SCAN_INTERVAL_MINUTES = env_int("SCAN_INTERVAL_MINUTES", 60)
    CATEGORY = os.getenv("BYBIT_CATEGORY", "linear")
    TOP_N = env_int("TOP_N", 100)
    LIMIT = env_int("KLINES_LIMIT", 200)

    MACD_FAST = env_int("MACD_FAST", 12)
    MACD_SLOW = env_int("MACD_SLOW", 26)
    MACD_SIGNAL = env_int("MACD_SIGNAL", 9)

    RSI_PERIOD = env_int("RSI_PERIOD", 14)
    ATR_PERIOD = env_int("ATR_PERIOD", 14)

    SLEEP_MS = env_int("PER_REQUEST_SLEEP_MS", 250)
    MAX_RETRIES = env_int("MAX_RETRIES", 3)
    RETRY_BACKOFF = env_int("RETRY_BACKOFF_SEC", 2)

    WORKERS = env_int("WORKERS", 8)
    USE_TICKERS_PREFILTER = env_int("USE_TICKERS_PREFILTER", 1)
    PREFILTER_MULTIPLIER = env_int("PREFILTER_MULTIPLIER", 1)

    # === dynamic TFs from .env ===
    TIMEFRAMES = parse_timeframes(os.getenv("TIMEFRAMES", "4H,1D,1W"))
    SORT_TF = check_sort_tf(os.getenv("SORT_TF", TIMEFRAMES[0]), TIMEFRAMES)
    logging.info(f"Активные таймфреймы: {', '.join(TIMEFRAMES)} | сортировка по ATR: {SORT_TF}")

    TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
    if not TG_TOKEN or not TG_CHAT_ID:
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID не заданы в .env")

    api_key = os.getenv("BYBIT_API_KEY") or ""
    api_secret = os.getenv("BYBIT_API_SECRET") or ""
    recv_window = env_int("BYBIT_RECV_WINDOW", 20000)

    tg = TelegramClient(TG_TOKEN, TG_CHAT_ID)
    api = BybitAPI(
        category=CATEGORY,
        sleep_ms=SLEEP_MS,
        max_retries=MAX_RETRIES,
        retry_backoff_sec=RETRY_BACKOFF,
        api_key=api_key,
        api_secret=api_secret,
        recv_window=recv_window,
    )

    while True:
        logging.info("=== Новый цикл ===")
        try:
            # 1) Все USDT-перпы
            instruments = api.get_instruments()
            symbols = [it["symbol"] for it in instruments]
            logging.info(f"Всего символов: {len(symbols)}")

            # 2) Быстрый префильтр по /tickers
            if USE_TICKERS_PREFILTER:
                tickers = api.get_tickers()
                tick_map = {t["symbol"]: t for t in tickers if t.get("symbol") in symbols}
                rows = []
                for sym, t in tick_map.items():
                    try:
                        high = float(t.get("highPrice24h") or t.get("highPrice") or 0)
                        low  = float(t.get("lowPrice24h")  or t.get("lowPrice")  or 0)
                        last = float(t.get("lastPrice") or 0)
                        if last <= 0 or high <= 0 or low <= 0:
                            continue
                        range_pct = (high - low) / last
                        rows.append({"symbol": sym, "range24h_pct": range_pct})
                    except Exception:
                        continue
                rows = sorted(rows, key=lambda x: x["range24h_pct"], reverse=True)
                pre_count = max(TOP_N * PREFILTER_MULTIPLIER, TOP_N)
                pre_top = [r["symbol"] for r in rows[:pre_count]]
                logging.info(f"Префильтр по /tickers: выбрано {len(pre_top)} (multiplier={PREFILTER_MULTIPLIER}).")
            else:
                pre_top = symbols

            # 3) Параллельно обрабатываем пары с выбранными ТФ
            def load_pair(sym: str):
                try:
                    trends = {}
                    rsis = {}
                    atr_abs = None
                    atr_pct = None

                    for tf in TIMEFRAMES:
                        interval = TF_TO_BYBIT[tf]
                        limit = min(LIMIT, 120) if interval in LONG_TF_CODES else LIMIT
                        kl = api.get_klines(sym, interval, limit=limit)
                        # Требования к числу баров: для недель/месяцев хватит 30, для остальных ≥ 50
                        if (interval in LONG_TF_CODES and len(kl) < 30) or (interval not in LONG_TF_CODES and len(kl) < 50):
                            return sym, None  # мало данных для одного из ТФ → пропуск пары

                        df = kline_to_df(kl)
                        need_atr = (tf == SORT_TF)
                        m_line, s_line, h_line, rsi_series, atr_series = compute_indicators(
                            df, MACD_FAST, MACD_SLOW, MACD_SIGNAL, RSI_PERIOD, ATR_PERIOD, need_atr
                        )
                        trend = classify_trend(m_line, s_line, h_line)
                        trends[tf] = trend
                        rsis[tf] = float(rsi_series.iloc[-1])

                        if need_atr:
                            last_close = float(df["close"].iloc[-1])
                            atr_abs = float(atr_series.iloc[-1]) if atr_series is not None else 0.0
                            atr_pct = (atr_abs / last_close) if last_close else 0.0

                    # Все выбранные ТФ должны иметь одинаковый тренд (BULL или BEAR)
                    uniq = set(trends.values())
                    if "NEUTRAL" in uniq or len(uniq) != 1:
                        return sym, None
                    common = uniq.pop()

                    payload = {
                        "common_trend": common,
                        "atr_sort_abs": atr_abs,
                        "atr_sort_pct": float(atr_pct or 0.0),
                        # расплющим RSI в виде rsi_4H, rsi_1D, ...
                        **{f"rsi_{tf}": rsis[tf] for tf in TIMEFRAMES},
                    }
                    return sym, payload
                except Exception as e:
                    logging.debug(f"[{sym}] ошибка загрузки/индикаторов: {e}")
                    return sym, None

            results = {}
            with ThreadPoolExecutor(max_workers=WORKERS) as ex:
                futs = [ex.submit(load_pair, s) for s in pre_top]
                for f in as_completed(futs):
                    sym, data = f.result()
                    if data:
                        results[sym] = data

            # 4) Делим на BULL/BEAR, ограничиваем по TOP_N (сортировка по ATR% выбранного SORT_TF)
            bull_list, bear_list = [], []
            for sym, d in results.items():
                row = {
                    "symbol": f"{sym.replace('USDT', '')}/USDT",
                    "atr_abs": d["atr_sort_abs"],
                    "atr_pct": d["atr_sort_pct"],
                    **{k: v for k, v in d.items() if k.startswith("rsi_")},
                }
                if d["common_trend"] == "BULL":
                    bull_list.append(row)
                elif d["common_trend"] == "BEAR":
                    bear_list.append(row)

            bull_list = sorted(bull_list, key=lambda x: x["atr_pct"], reverse=True)[:TOP_N]
            bear_list = sorted(bear_list, key=lambda x: x["atr_pct"], reverse=True)[:TOP_N]
            logging.info(f"Финальный отбор ({'+'.join(TIMEFRAMES)}): BULL={len(bull_list)} BEAR={len(bear_list)}")

            # 5) Open Interest — только для финальных списков
            def add_oi(item):
                sym = item["symbol"].replace("/USDT", "USDT")
                try:
                    oi = api.get_open_interest(sym, interval="1h") or 0.0
                except Exception:
                    oi = 0.0
                item["oi"] = oi
                return item

            with ThreadPoolExecutor(max_workers=min(6, WORKERS)) as ex:
                bull_list = list(ex.map(add_oi, bull_list))
                bear_list = list(ex.map(add_oi, bear_list))

            # 6) Итоговая сортировка для отчёта — по абсолютному ATR выбранного SORT_TF (сильнее — выше)
            bull_sorted = sorted(bull_list, key=lambda x: x["atr_abs"], reverse=True)
            bear_sorted = sorted(bear_list, key=lambda x: x["atr_abs"], reverse=True)

            # 7) Формируем .txt и сохраняем
            report_text = build_report_txt(
                bull_sorted,
                bear_sorted,
                timeframes=TIMEFRAMES,
                sort_tf=SORT_TF,
                tz="Europe/Kyiv",
            )
            filepath = write_report_file(report_text)

            # 8) История (JSON) — кладём и набор ТФ
            append_history({"bull": bull_sorted, "bear": bear_sorted, "timeframes": TIMEFRAMES, "sort_tf": SORT_TF})

            # 9) Telegram
            tg.send_document(filepath, caption=f"BYBIT MACD Scanner — отчёт ({' & '.join(TIMEFRAMES)})")

        except Exception as e:
            logging.exception(f"Фатальная ошибка цикла: {e}")

        logging.info(f"Сон на {SCAN_INTERVAL_MINUTES} мин...")
        time.sleep(SCAN_INTERVAL_MINUTES * 60)


if __name__ == "__main__":
    main_loop()
