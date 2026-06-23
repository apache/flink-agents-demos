"""模拟股票行情数据生成器。

所有数据为模拟数据，使用确定性随机种子保证可复现。
"""

import json
import random
from typing import Dict, List

from models import StockTick

# 基准价格（模拟）
BASE_PRICES = {
    "AAPL": 185.0,
    "GOOGL": 175.0,
    "TSLA": 250.0,
    "MSFT": 420.0,
    "NVDA": 130.0,
}


def generate_price_history(symbol: str, num_points: int = 30) -> List[float]:
    """生成一只股票的历史价格序列（供 RSI/MACD 工具使用）。"""
    rng = random.Random(hash(symbol) & 0xFFFFFFFF)
    base = BASE_PRICES.get(symbol, 100.0)
    prices = []
    price = base * 0.95
    for _ in range(num_points):
        change = rng.gauss(0, base * 0.015)
        price = max(price + change, base * 0.7)
        prices.append(round(price, 2))
    return prices


def generate_stock_ticks(
    symbols: List[str] | None = None, num_ticks: int = 1
) -> List[Dict]:
    """生成 from_list 格式的行情输入数据。

    返回格式: [{"key": "AAPL", "value": StockTick(...)}, ...]
    """
    if symbols is None:
        symbols = ["AAPL", "GOOGL", "TSLA"]

    rng = random.Random(42)
    ticks = []

    for symbol in symbols:
        base = BASE_PRICES.get(symbol, 100.0)
        for i in range(num_ticks):
            price = round(base * (1 + rng.gauss(0, 0.03)), 2)
            high = round(price * (1 + abs(rng.gauss(0, 0.01))), 2)
            low = round(price * (1 - abs(rng.gauss(0, 0.01))), 2)
            open_price = round(price * (1 + rng.gauss(0, 0.005)), 2)
            volume = int(abs(rng.gauss(5_000_000, 2_000_000)))

            tick = StockTick(
                symbol=symbol,
                price=price,
                volume=volume,
                high=high,
                low=low,
                open_price=open_price,
                timestamp=f"2026-06-12T09:{30 + i:02d}:00",
            )
            ticks.append({"key": symbol, "value": tick})

    return ticks


# 价格历史缓存（供工具函数查询）
PRICE_HISTORIES = {sym: generate_price_history(sym) for sym in BASE_PRICES}


def get_price_history_json(symbol: str) -> str:
    """获取指定股票的历史价格 JSON 字符串。"""
    prices = PRICE_HISTORIES.get(symbol, generate_price_history(symbol))
    return json.dumps(prices)
