"""数据源代理模块。

统一接口，按 --source 参数委托到 mock_data 或 real_data。
所有消费方通过 `from data_source import generate_stock_ticks, ...` 使用，
无需关心底层数据源。
"""

import argparse
from typing import Dict, List

_source = "demo"


def set_source(source: str):
    global _source
    _source = source


def _get_module():
    if _source == "real":
        import real_data
        return real_data
    import mock_data
    return mock_data


def generate_stock_ticks(
    symbols: List[str] | None = None, num_ticks: int = 1
) -> List[Dict]:
    return _get_module().generate_stock_ticks(symbols, num_ticks)


def generate_price_history(symbol: str, num_points: int = 30) -> List[float]:
    return _get_module().generate_price_history(symbol, num_points)


def get_price_history_json(symbol: str) -> str:
    return _get_module().get_price_history_json(symbol)


def get_stock_news(symbol: str) -> list:
    if _source == "real":
        import real_data
        return real_data.get_stock_news(symbol)
    return None


def clear_cache():
    """清除数据缓存（仅 real 模式有效）。"""
    if _source == "real":
        import real_data
        real_data.clear_cache()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source",
        choices=["demo", "real"],
        default="demo",
        help="数据源: demo(模拟数据) 或 real(AKShare美股真实行情)",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=None,
        help="股票代码列表, 例如: AAPL TSLA GOOGL",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=0,
        help="持续监控间隔(秒), 例如: 30。不指定则运行一次后退出",
    )
    return parser.parse_args()
