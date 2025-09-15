# reporter.py
import os
from typing import List, Dict
from datetime import datetime
import pytz

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "output")
LAST_REPORT = os.path.join(OUTPUT_DIR, "last_report.txt")

def ensure_output_dir():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR, exist_ok=True)

def format_float(x, nd=2):
    try:
        return f"{float(x):.{nd}f}"
    except Exception:
        return "-"

def _rsi_line(item: Dict, timeframes: List[str]) -> str:
    parts = []
    for tf in timeframes:
        val = item.get(f"rsi_{tf}")
        parts.append(f"{tf}: {format_float(val, 2)}")
    return " | ".join(parts)

def build_report_txt(
    bull_list: List[Dict],
    bear_list: List[Dict],
    timeframes: List[str],
    sort_tf: str,
    tz: str = "UTC",
) -> str:
    dt = datetime.now(pytz.timezone(tz)).strftime("%Y-%m-%d %H:%M:%S %Z")
    tf_title = ", ".join(timeframes)
    lines = []
    lines.append(f"BYBIT MACD SCANNER ({tf_title}) — {dt}")
    lines.append("Правила: выбранные ТФ должны иметь одинаковый тренд MACD (BULL или BEAR).")
    lines.append(f"Списки отсортированы по ATR {sort_tf} (по убыванию).")
    lines.append("────────────────────────────────────────────────────────")
    lines.append("")

    def section(title: str, items: List[Dict]):
        lines.append(title)
        lines.append("────────────────────────────────────────────────────────")
        if not items:
            lines.append("(пусто)")
            lines.append("")
            return
        for i, it in enumerate(items, 1):
            sym = it["symbol"]
            atr_pct = it.get("atr_pct") or 0.0
            oi = it.get("oi", 0.0)

            lines.append(f"{i:02d}. {sym}")
            lines.append(f"    • ATR {sort_tf}: {format_float(atr_pct * 100, 2)}%")
            lines.append(f"    • RSI: {_rsi_line(it, timeframes)}")
            try:
                if oi and float(oi) > 0:
                    lines.append(f"    • OI: {format_float(oi, 2)}")
            except Exception:
                pass
            lines.append("")

    section("BULL:", bull_list)
    section("BEAR:", bear_list)

    return "\n".join(lines)

def write_report_file(text: str) -> str:
    ensure_output_dir()
    with open(LAST_REPORT, "w", encoding="utf-8") as f:
        f.write(text)
    return LAST_REPORT
