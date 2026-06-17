"""数据模型定义 — 股票行情、交易决策、投资组合。"""

from enum import Enum
from typing import List

from pydantic import BaseModel, Field


class TradingAction(str, Enum):
    """交易动作枚举。"""

    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"


class StockTick(BaseModel):
    """单条股票行情数据。"""

    symbol: str
    price: float
    volume: int
    high: float
    low: float
    open_price: float
    timestamp: str
    price_history: str = ""


class TradingDecision(BaseModel):
    """LLM 输出的交易决策（用作 ReActAgent 的 output_schema）。"""

    symbol: str = Field(description="股票代码")
    action: TradingAction = Field(description="交易动作: buy/sell/hold")
    confidence: float = Field(description="置信度 0.0-1.0")
    quantity: int = Field(description="建议交易数量")
    reason: str = Field(description="决策理由")
    risk_level: str = Field(description="风险等级: low/medium/high")


class PortfolioPosition(BaseModel):
    """单只股票的持仓信息。"""

    symbol: str
    shares: int
    avg_cost: float
    current_price: float
    unrealized_pnl: float
