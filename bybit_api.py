# bybit_api.py
import time
import logging
import math
import hmac
import hashlib
from typing import List, Dict, Any, Optional
import requests
from tenacity import retry, stop_after_attempt, wait_fixed

BYBIT_BASE = "https://api.bybit.com"  # v5 unified

def _hmac_sha256(secret: str, payload: str) -> str:
    return hmac.new(secret.encode('utf-8'), payload.encode('utf-8'), hashlib.sha256).hexdigest()

class BybitAPI:
    def __init__(
        self,
        category: str = "linear",
        sleep_ms: int = 250,
        max_retries: int = 3,
        retry_backoff_sec: int = 2,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        recv_window: int = 20000,
    ):
        self.category = category
        self.sleep_ms = sleep_ms
        self.session = requests.Session()
        self.max_retries = max_retries
        self.retry_backoff_sec = retry_backoff_sec
        self.api_key = api_key or ""
        self.api_secret = api_secret or ""
        self.recv_window = recv_window

    def _sleep(self):
        time.sleep(self.sleep_ms / 1000.0)

    def _check(self, r: requests.Response) -> Dict[str, Any]:
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code}: {r.text}")
        data = r.json()
        # retCode != 0 => ошибка/лимит/и пр.
        if str(data.get("retCode")) != "0":
            raise RuntimeError(f"Bybit error retCode={data.get('retCode')} retMsg={data.get('retMsg')} data={data}")
        return data

    def _headers_public(self) -> Dict[str, str]:
        # Для публичных v5 подпись не обязательна; можем просто добавить ключ
        h = {"Accept": "application/json"}
        if self.api_key:
            h["X-BAPI-API-KEY"] = self.api_key
        return h

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def get_instruments(self) -> List[Dict[str, Any]]:
        """
        Все USDT линейные перпы со статусом Trading.
        Пагинация через nextPageCursor.
        """
        url = f"{BYBIT_BASE}/v5/market/instruments-info"
        params = {"category": self.category, "limit": 1000}
        out: List[Dict[str, Any]] = []
        cursor = None

        while True:
            if cursor:
                params["cursor"] = cursor
            r = self.session.get(url, params=params, headers=self._headers_public(), timeout=20)
            data = self._check(r)
            result = data.get("result", {}) or {}
            items = result.get("list", []) or []

            for it in items:
                status = str(it.get("status", "")).lower()          # "trading"
                symbol = it.get("symbol", "") or ""
                quote = (it.get("quoteCoin") or it.get("quoteSymbol") or "").upper()
                settle = (it.get("settleCoin") or "").upper()
                contract_type = str(it.get("contractType", "")).lower()

                is_usdt = ("USDT" in quote) or ("USDT" in settle) or symbol.endswith("USDT")
                is_trading = ("trading" in status)
                is_linear_perp = True
                if self.category == "linear" and contract_type:
                    is_linear_perp = ("perpetual" in contract_type)

                if is_usdt and is_trading and is_linear_perp:
                    out.append(it)

            cursor = result.get("nextPageCursor")
            if not cursor:
                break
            self._sleep()

        logging.info(f"Найдено {len(out)} торгуемых USDT-перпетуалов.")
        self._sleep()
        return out

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def get_tickers(self) -> List[Dict[str, Any]]:
        """
        /v5/market/tickers — один запрос, возвращает 24h high/low/last и др.
        Используем для быстрого префильтра по 24h range%.
        """
        url = f"{BYBIT_BASE}/v5/market/tickers"
        params = {"category": self.category}
        r = self.session.get(url, params=params, headers=self._headers_public(), timeout=20)
        data = self._check(r)
        items = data.get("result", {}).get("list", []) or []
        self._sleep()
        return items

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def get_klines(self, symbol: str, interval: str, limit: int = 200) -> List[Dict[str, Any]]:
        """
        /v5/market/kline
        interval: 1,3,5,15,30,60,120,240,360,720,D,W,M
        """
        url = f"{BYBIT_BASE}/v5/market/kline"
        params = {
            "category": self.category,
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
        r = self.session.get(url, params=params, headers=self._headers_public(), timeout=20)
        data = self._check(r)
        lst = data.get("result", {}).get("list", []) or []
        lst_sorted = sorted(lst, key=lambda x: int(x[0]))
        self._sleep()
        return [
            {
                "ts": int(x[0]),
                "open": float(x[1]),
                "high": float(x[2]),
                "low": float(x[3]),
                "close": float(x[4]),
                "volume": float(x[5]),
                "turnover": float(x[6]) if len(x) > 6 and x[6] not in (None, "") else math.nan,
            } for x in lst_sorted
        ]

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def get_open_interest(self, symbol: str, interval: str = "1h") -> Optional[float]:
        """
        /v5/market/open-interest (public). Возвращаем последнее значение.
        """
        url = f"{BYBIT_BASE}/v5/market/open-interest"
        params = {
            "category": self.category,
            "symbol": symbol,
            "interval": interval,
            "limit": 1
        }
        r = self.session.get(url, params=params, headers=self._headers_public(), timeout=15)
        data = self._check(r)
        lst = data.get("result", {}).get("list", []) or []
        self._sleep()
        if not lst:
            return None
        try:
            return float(lst[-1].get("openInterest"))
        except Exception:
            return None
