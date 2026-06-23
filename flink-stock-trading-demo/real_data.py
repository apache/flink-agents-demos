"""真实股票行情数据获取（AKShare）。

与 mock_data.py 接口一致，使用 AKShare 获取美股真实行情。
- 美股实时/历史日线: stock_us_daily（Sina 源）
- 股票新闻: stock_news_em
"""

import json
import time
from datetime import datetime
from typing import Dict, List

import akshare as ak

from models import StockTick

_price_history_cache: Dict[str, List[float]] = {}


def clear_cache():
    """清除价格历史缓存，确保下一次调用获取最新数据。"""
    _price_history_cache.clear()


def _fetch_us_latest(symbol: str) -> dict:
    """获取美股最新日线数据（取最后一行）。"""
    try:
        df = ak.stock_us_daily(symbol=symbol, adjust="")
        if df.empty:
            return {}
        row = df.iloc[-1]
        return {
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": int(row["volume"]),
            "date": str(row["date"]),
        }
    except Exception as e:
        print(f"  [警告] 获取 {symbol} 美股行情失败: {e}")
        return {}


def generate_stock_ticks(
    symbols: List[str] | None = None, num_ticks: int = 1
) -> List[Dict]:
    """获取美股真实行情数据，返回与 mock_data 相同的格式。

    num_ticks 参数保留兼容性但真实数据只返回1条。
    """
    if symbols is None:
        symbols = ["AAPL", "TSLA"]

    ticks = []

    for symbol in symbols:
        data = _fetch_us_latest(symbol)
        if not data:
            print(f"  [跳过] {symbol}: 无法获取数据")
            continue
        tick = StockTick(
            symbol=symbol,
            price=data["close"],
            volume=data["volume"],
            high=data["high"],
            low=data["low"],
            open_price=data["open"],
            timestamp=data.get("date", datetime.now().strftime("%Y-%m-%dT%H:%M:%S")),
        )
        ticks.append({"key": symbol, "value": tick})
        time.sleep(0.5)

    return ticks


def generate_price_history(symbol: str, num_points: int = 30) -> List[float]:
    """获取美股真实历史收盘价（日线级别）。"""
    try:
        df = ak.stock_us_daily(symbol=symbol, adjust="")
        prices = df["close"].tail(num_points).tolist()
        return [round(float(p), 2) for p in prices]
    except Exception as e:
        print(f"  [警告] 获取 {symbol} 历史价格失败: {e}")
        return [100.0] * num_points


def get_price_history_json(symbol: str) -> str:
    """获取历史价格 JSON 字符串（带缓存）。"""
    if symbol not in _price_history_cache:
        _price_history_cache[symbol] = generate_price_history(symbol)
    return json.dumps(_price_history_cache[symbol])


_POSITIVE_KEYWORDS = [
    "增长", "上涨", "利好", "突破", "创新高", "超预期", "大涨", "强劲",
    "盈利", "买入", "增持", "上调", "回暖", "反弹", "新高", "看好",
    "加速", "扩张", "丰收", "提升",
]
_NEGATIVE_KEYWORDS = [
    "下跌", "利空", "下调", "减持", "亏损", "暴跌", "风险", "承压",
    "下滑", "缩水", "卖出", "警告", "处罚", "违规", "退市", "低迷",
    "萎缩", "收缩", "恶化", "负增长",
]


def _infer_sentiment(title: str) -> str:
    pos = sum(1 for kw in _POSITIVE_KEYWORDS if kw in title)
    neg = sum(1 for kw in _NEGATIVE_KEYWORDS if kw in title)
    if pos > neg:
        return "positive"
    elif neg > pos:
        return "negative"
    return "neutral"


def get_stock_news(symbol: str) -> list:
    """通过 AKShare 获取真实股票新闻，返回新闻列表。

    返回格式: [{"title": "...", "sentiment": "positive/negative/neutral"}, ...]
    """
    try:
        import pandas as pd
        pd.set_option("string_storage", "python")
        df = ak.stock_news_em(symbol=symbol)
        if df.empty:
            return [{"title": f"{symbol} 暂无最新新闻", "sentiment": "neutral"}]
        news = []
        for _, row in df.head(5).iterrows():
            title = str(row.get("新闻标题", ""))
            sentiment = _infer_sentiment(title)
            news.append({"title": title, "sentiment": sentiment})
        return news
    except Exception as e:
        print(f"  [警告] 获取 {symbol} 新闻失败: {e}")
        return [{"title": f"{symbol} 暂无最新新闻", "sentiment": "neutral"}]
