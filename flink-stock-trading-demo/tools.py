"""股票分析工具函数 — 全部使用模拟数据，无需外部 API。

每个函数使用 numpydoc 格式的 docstring，
框架通过 docstring_parser 自动提取参数描述作为工具元数据。
"""

import json
from typing import List


def calculate_rsi(prices_json: str, period: int = 14) -> str:
    """计算股票的 RSI（相对强弱指数）技术指标。

    Parameters
    ----------
    prices_json : str
        历史价格列表的 JSON 字符串，例如 "[100.0, 101.5, 99.8, ...]"
    period : int
        RSI 计算周期，默认 14

    Returns
    -------
    str
        包含 RSI 值和解读的 JSON 字符串
    """
    try:
        prices: List[float] = json.loads(prices_json)
    except (json.JSONDecodeError, TypeError):
        return json.dumps({"error": "无法解析价格数据", "rsi": 50.0})

    if len(prices) < period + 1:
        return json.dumps({"error": "数据点不足", "rsi": 50.0})

    gains, losses = [], []
    for i in range(1, len(prices)):
        change = prices[i] - prices[i - 1]
        gains.append(max(change, 0))
        losses.append(max(-change, 0))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        rsi = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

    rsi = round(rsi, 2)
    if rsi < 30:
        interpretation = "超卖区间，可能存在反弹机会"
    elif rsi > 70:
        interpretation = "超买区间，可能面临回调风险"
    else:
        interpretation = "中性区间，趋势不明显"

    return json.dumps(
        {"rsi": rsi, "period": period, "interpretation": interpretation},
        ensure_ascii=False,
    )


def calculate_macd(prices_json: str) -> str:
    """计算股票的 MACD（指数平滑异同移动平均线）指标。

    Parameters
    ----------
    prices_json : str
        历史价格列表的 JSON 字符串

    Returns
    -------
    str
        包含 MACD 线、信号线、柱状图的 JSON 字符串
    """
    try:
        prices: List[float] = json.loads(prices_json)
    except (json.JSONDecodeError, TypeError):
        return json.dumps({"error": "无法解析价格数据"})

    def ema(data: List[float], span: int) -> List[float]:
        multiplier = 2.0 / (span + 1)
        result = [data[0]]
        for val in data[1:]:
            result.append(val * multiplier + result[-1] * (1 - multiplier))
        return result

    if len(prices) < 26:
        return json.dumps({"error": "数据点不足，MACD需要至少26个数据点"})

    ema12 = ema(prices, 12)
    ema26 = ema(prices, 26)
    macd_line = [round(e12 - e26, 4) for e12, e26 in zip(ema12, ema26)]
    signal_line = ema(macd_line, 9)
    histogram = [round(m - s, 4) for m, s in zip(macd_line, signal_line)]

    current_macd = round(macd_line[-1], 4)
    current_signal = round(signal_line[-1], 4)
    current_hist = round(histogram[-1], 4)

    if current_macd > current_signal and current_hist > 0:
        trend = "看涨信号：MACD线在信号线上方且柱状图为正"
    elif current_macd < current_signal and current_hist < 0:
        trend = "看跌信号：MACD线在信号线下方且柱状图为负"
    else:
        trend = "趋势不明朗，建议观望"

    return json.dumps(
        {
            "macd_line": current_macd,
            "signal_line": current_signal,
            "histogram": current_hist,
            "trend": trend,
        },
        ensure_ascii=False,
    )


def get_stock_news(symbol: str) -> str:
    """获取指定股票的最新市场新闻摘要。

    Parameters
    ----------
    symbol : str
        股票代码，例如 "AAPL"、"GOOGL"

    Returns
    -------
    str
        包含新闻标题和情绪分析的 JSON 字符串
    """
    import data_source

    real_news = data_source.get_stock_news(symbol)
    if real_news is not None:
        news = real_news
    else:
        news_db = {
            "AAPL": [
                {"title": "苹果发布 Vision Pro 2，市场反应积极", "sentiment": "positive"},
                {"title": "iPhone 销量超预期，大中华区增长强劲", "sentiment": "positive"},
                {"title": "分析师上调苹果目标价至 220 美元", "sentiment": "positive"},
            ],
            "GOOGL": [
                {"title": "谷歌 AI 搜索份额持续增长", "sentiment": "positive"},
                {"title": "反垄断案判决可能影响广告业务", "sentiment": "negative"},
                {"title": "云计算收入同比增长 28%", "sentiment": "positive"},
            ],
            "TSLA": [
                {"title": "特斯拉自动驾驶获得新市场监管批准", "sentiment": "positive"},
                {"title": "电动车价格战加剧利润率承压", "sentiment": "negative"},
                {"title": "储能业务成为新增长引擎", "sentiment": "positive"},
            ],
            "MSFT": [
                {"title": "微软 Azure AI 服务需求激增", "sentiment": "positive"},
                {"title": "Copilot 企业版订阅量翻倍", "sentiment": "positive"},
            ],
            "NVDA": [
                {"title": "英伟达下一代 GPU 供不应求", "sentiment": "positive"},
                {"title": "AI 芯片出口限制可能扩大", "sentiment": "negative"},
            ],
        }
        news = news_db.get(symbol, [{"title": f"{symbol} 暂无最新新闻", "sentiment": "neutral"}])

    positive = sum(1 for n in news if n["sentiment"] == "positive")
    negative = sum(1 for n in news if n["sentiment"] == "negative")
    overall = "偏正面" if positive > negative else "偏负面" if negative > positive else "中性"

    return json.dumps(
        {"symbol": symbol, "news": news, "overall_sentiment": overall},
        ensure_ascii=False,
    )


_portfolio_db = {
    "AAPL": {"shares": 50, "avg_cost": 178.50, "current_price": 185.0},
    "GOOGL": {"shares": 0, "avg_cost": 0, "current_price": 175.0},
    "TSLA": {"shares": 20, "avg_cost": 260.00, "current_price": 250.0},
}


def check_portfolio(symbol: str) -> str:
    """查询指定股票的当前持仓信息。

    Parameters
    ----------
    symbol : str
        股票代码

    Returns
    -------
    str
        包含持仓数量、成本、盈亏的 JSON 字符串
    """
    pos = _portfolio_db.get(symbol, {"shares": 0, "avg_cost": 0, "current_price": 0})
    pnl = round((pos["current_price"] - pos["avg_cost"]) * pos["shares"], 2) if pos["shares"] > 0 else 0

    return json.dumps(
        {
            "symbol": symbol,
            "shares": pos["shares"],
            "avg_cost": pos["avg_cost"],
            "unrealized_pnl": pnl,
            "position_value": round(pos["current_price"] * pos["shares"], 2),
        },
        ensure_ascii=False,
    )


def get_portfolio_state(symbol: str) -> dict:
    """获取指定股票的当前持仓状态（供 Agent 写入 short_term_memory）。

    Parameters
    ----------
    symbol : str
        股票代码

    Returns
    -------
    dict
        {"shares": int, "avg_cost": float}
    """
    pos = _portfolio_db.get(symbol, {"shares": 0, "avg_cost": 0})
    return {"shares": pos["shares"], "avg_cost": pos["avg_cost"]}


def execute_trade(symbol: str, action: str, quantity: int, price: float) -> str:
    """执行模拟交易下单。

    Parameters
    ----------
    symbol : str
        股票代码
    action : str
        交易方向: "buy" 或 "sell"
    quantity : int
        交易数量
    price : float
        交易价格

    Returns
    -------
    str
        交易确认信息的 JSON 字符串
    """
    if symbol not in _portfolio_db:
        _portfolio_db[symbol] = {"shares": 0, "avg_cost": 0.0, "current_price": price}

    pos = _portfolio_db[symbol]
    pos["current_price"] = price

    if action == "buy":
        old_shares = pos["shares"]
        old_avg = pos["avg_cost"]
        new_shares = old_shares + quantity
        pos["avg_cost"] = round((old_avg * old_shares + price * quantity) / new_shares, 2)
        pos["shares"] = new_shares
    elif action == "sell":
        pos["shares"] = max(0, pos["shares"] - quantity)
        if pos["shares"] == 0:
            pos["avg_cost"] = 0.0

    total = round(price * quantity, 2)
    return json.dumps(
        {
            "status": "executed",
            "symbol": symbol,
            "action": action,
            "quantity": quantity,
            "price": price,
            "total_amount": total,
            "order_id": f"SIM-{symbol}-{action.upper()}-{quantity}",
            "message": f"模拟交易已执行: {action} {quantity} 股 {symbol} @ ${price}",
        },
        ensure_ascii=False,
    )
