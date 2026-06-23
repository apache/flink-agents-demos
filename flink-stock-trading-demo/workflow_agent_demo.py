"""Demo 2: Workflow Agent — 多步骤股票交易决策 Agent。

展示能力:
  - Workflow Agent 模式（自定义 Action 链）
  - @tool / @prompt / @chat_model_setup / @chat_model_connection 装饰器
  - 短期记忆（short_term_memory 跨 tick 跟踪持仓）
  - 感知记忆（sensory_memory 区分多阶段响应）
  - 多步骤事件链（技术分析 → 交易决策）
  - Skills 系统（market-screener 技能）
  - 持续监控模式（--interval N 秒循环获取最新数据）

事件流:
  InputEvent → [process_input] → ChatRequestEvent(technical_model)
    → [内置 ChatModelAction: LLM + 工具调用] → ChatResponseEvent
    → [process_response 第1阶段] → ChatRequestEvent(decision_model)
    → [内置 ChatModelAction: LLM + 工具调用] → ChatResponseEvent
    → [process_response 第2阶段] → OutputEvent

运行:
  export DASHSCOPE_API_KEY=your_key
  python workflow_agent_demo.py                          # 模拟数据（默认）
  python workflow_agent_demo.py --source real             # AKShare 真实行情
  python workflow_agent_demo.py --source real --symbols AAPL NVDA
  python workflow_agent_demo.py --source real --symbols NVDA --interval 60  # 持续监控
"""

import json
import time
from datetime import datetime
from pathlib import Path

from flink_agents.api.agents.agent import Agent
from flink_agents.api.chat_message import ChatMessage, MessageRole
from flink_agents.api.decorators import (
    action,
    chat_model_connection,
    chat_model_setup,
    prompt,
    skills,
    tool,
)
from flink_agents.api.events.chat_event import ChatRequestEvent, ChatResponseEvent
from flink_agents.api.events.event import Event, InputEvent, OutputEvent
from flink_agents.api.execution_environment import AgentsExecutionEnvironment
from flink_agents.api.prompts.prompt import Prompt
from flink_agents.api.resource import ResourceDescriptor, ResourceName
from flink_agents.api.runner_context import RunnerContext
from flink_agents.api.skills import Skills

from config import TONGYI_CONNECTION_NAME, TONGYI_MODEL, TONGYI_TEMPERATURE
from data_source import clear_cache, generate_stock_ticks, get_price_history_json, parse_args, set_source
from prompts import workflow_decision_prompt, workflow_technical_prompt
from tools import (
    calculate_macd,
    calculate_rsi,
    check_portfolio,
    execute_trade,
    get_portfolio_state,
)

SKILLS_DIR = str(Path(__file__).parent / "skills")


class StockTradingAgent(Agent):
    """多步骤股票交易决策 Agent。

    两阶段处理链:
      阶段1: 技术分析（technical_model + RSI/MACD/持仓查询工具）
      阶段2: 交易决策（decision_model + 交易执行工具）
    """

    # ================================================================
    # 资源声明
    # ================================================================

    @chat_model_connection
    @staticmethod
    def tongyi_conn() -> ResourceDescriptor:
        """通义千问 LLM 连接。"""
        return ResourceDescriptor(clazz=ResourceName.ChatModel.TONGYI_CONNECTION)

    @prompt
    @staticmethod
    def technical_prompt() -> Prompt:
        """技术分析提示词。"""
        return workflow_technical_prompt

    @prompt
    @staticmethod
    def decision_prompt() -> Prompt:
        """交易决策提示词。"""
        return workflow_decision_prompt

    @tool
    @staticmethod
    def calculate_rsi(prices_json: str, period: int = 14) -> str:
        """计算股票的 RSI 技术指标。

        Parameters
        ----------
        prices_json : str
            历史价格列表的 JSON 字符串
        period : int
            RSI 计算周期，默认 14
        """
        return calculate_rsi(prices_json, period)

    @tool
    @staticmethod
    def calculate_macd(prices_json: str) -> str:
        """计算股票的 MACD 技术指标。

        Parameters
        ----------
        prices_json : str
            历史价格列表的 JSON 字符串
        """
        return calculate_macd(prices_json)

    @tool
    @staticmethod
    def check_portfolio(symbol: str) -> str:
        """查询指定股票的当前持仓信息。

        Parameters
        ----------
        symbol : str
            股票代码
        """
        return check_portfolio(symbol)

    @tool
    @staticmethod
    def execute_trade(symbol: str, action: str, quantity: int, price: float) -> str:
        """执行模拟交易下单。

        Parameters
        ----------
        symbol : str
            股票代码
        action : str
            交易方向: buy 或 sell
        quantity : int
            交易数量
        price : float
            交易价格
        """
        return execute_trade(symbol, action, quantity, price)

    @skills
    @staticmethod
    def my_skills() -> Skills:
        """加载本地技能包（market-screener）。"""
        return Skills.from_local_dir(SKILLS_DIR)

    @chat_model_setup
    @staticmethod
    def technical_model() -> ResourceDescriptor:
        """技术分析模型 — 配备 RSI/MACD/持仓查询工具 + 技能。"""
        return ResourceDescriptor(
            clazz=ResourceName.ChatModel.TONGYI_SETUP,
            connection=TONGYI_CONNECTION_NAME,
            model=TONGYI_MODEL,
            temperature=TONGYI_TEMPERATURE,
            prompt="technical_prompt",
            tools=["calculate_rsi", "calculate_macd", "check_portfolio"],
            skills=["market-screener"],
            allowed_commands=["echo"],
        )

    @chat_model_setup
    @staticmethod
    def decision_model() -> ResourceDescriptor:
        """交易决策模型 — 配备交易执行工具。"""
        return ResourceDescriptor(
            clazz=ResourceName.ChatModel.TONGYI_SETUP,
            connection=TONGYI_CONNECTION_NAME,
            model=TONGYI_MODEL,
            temperature=TONGYI_TEMPERATURE,
            prompt="decision_prompt",
            tools=["execute_trade"],
        )

    # ================================================================
    # 事件处理 Actions
    # ================================================================

    @action(InputEvent.EVENT_TYPE)
    @staticmethod
    def process_input(event: Event, ctx: RunnerContext) -> None:
        """阶段0: 接收行情数据，发起技术分析请求。"""
        input_data = InputEvent.from_event(event).input
        if hasattr(input_data, "model_dump"):
            tick_dict = input_data.model_dump()
        elif isinstance(input_data, dict):
            tick_dict = input_data
        else:
            tick_dict = {"raw": str(input_data)}

        symbol = tick_dict.get("symbol", "UNKNOWN")

        # 存入感知记忆（本次运行内有效）
        ctx.sensory_memory.set("tick", json.dumps(tick_dict, ensure_ascii=False))
        ctx.sensory_memory.set("symbol", symbol)

        # 附加历史价格供工具使用
        prices_json = get_price_history_json(symbol)
        tick_info = json.dumps(tick_dict, ensure_ascii=False)
        content = f"行情数据: {tick_info}\n历史价格(供技术指标计算): {prices_json}"

        # 发送 ChatRequest 到技术分析模型
        ctx.send_event(
            ChatRequestEvent(
                model="technical_model",
                messages=[ChatMessage(role=MessageRole.USER)],
                prompt_args={"input": content},
            )
        )

    @action(ChatResponseEvent.EVENT_TYPE)
    @staticmethod
    def process_response(event: Event, ctx: RunnerContext) -> None:
        """处理 LLM 响应 — 用感知记忆区分阶段。

        第1次响应: 技术分析完成 → 发起交易决策请求
        第2次响应: 交易决策完成 → 更新持仓 → 输出结果
        """
        chat_response = ChatResponseEvent.from_event(event)
        response_content = chat_response.response.content

        if not ctx.sensory_memory.is_exist("technical_analysis"):
            # ---- 阶段1: 技术分析完成 ----
            ctx.sensory_memory.set("technical_analysis", response_content)

            symbol = ctx.sensory_memory.get("symbol")
            tick = ctx.sensory_memory.get("tick")

            # 从短期记忆读取历史持仓（跨 tick 持久化）；首条 tick 回退到工具层初始值
            portfolio_info = "无持仓记录"
            if ctx.short_term_memory.is_exist("shares"):
                shares = ctx.short_term_memory.get("shares")
                avg_cost = ctx.short_term_memory.get("avg_cost")
                if shares > 0:
                    portfolio_info = f"持有 {shares} 股，均价 ${avg_cost}"
            else:
                portfolio = get_portfolio_state(symbol)
                if portfolio["shares"] > 0:
                    portfolio_info = f"持有 {portfolio['shares']} 股，均价 ${portfolio['avg_cost']}"

            # 发送 ChatRequest 到决策模型
            ctx.send_event(
                ChatRequestEvent(
                    model="decision_model",
                    messages=[ChatMessage(role=MessageRole.USER)],
                    prompt_args={
                        "technical_analysis": response_content,
                        "portfolio": portfolio_info,
                        "tick": tick,
                    },
                )
            )
        else:
            # ---- 阶段2: 交易决策完成 ----
            symbol = ctx.sensory_memory.get("symbol")

            # 更新短期记忆中的持仓状态
            ctx.short_term_memory.set("last_decision", response_content)
            portfolio = get_portfolio_state(symbol)
            ctx.short_term_memory.set("shares", portfolio["shares"])
            ctx.short_term_memory.set("avg_cost", portfolio["avg_cost"])

            # 构造最终输出
            technical = ctx.sensory_memory.get("technical_analysis")
            output = {
                "symbol": symbol,
                "technical_analysis": technical[:200] + "..."
                if len(technical) > 200
                else technical,
                "trading_decision": response_content,
            }

            ctx.send_event(OutputEvent(output=json.dumps(output, ensure_ascii=False)))


def _run_once(symbols: list[str], round_num: int = 0):
    """执行一轮分析。round_num > 0 时打印轮次信息。"""
    if round_num > 0:
        print(f"\n{'=' * 60}")
        print(f"  第 {round_num} 轮监控 — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

    env = AgentsExecutionEnvironment.get_execution_environment()
    input_list = generate_stock_ticks(symbols, num_ticks=1)

    if not input_list:
        print("  [跳过] 未获取到行情数据")
        return

    print(f"\n输入 {len(input_list)} 条行情数据:")
    for item in input_list:
        tick = item["value"]
        print(f"  [{item['key']}] 价格={tick.price} 成交量={tick.volume}")

    output_list = env.from_list(input_list).apply(StockTradingAgent()).to_list()
    env.execute()

    print(f"\n{'=' * 60}")
    print("  交易决策结果:")
    print("=" * 60)
    for output in output_list:
        for key, value in output.items():
            print(f"\n  [{key}]")
            try:
                parsed = json.loads(value)
                print(f"    技术分析: {parsed.get('technical_analysis', 'N/A')}")
                print(f"    交易决策: {parsed.get('trading_decision', 'N/A')}")
            except (json.JSONDecodeError, TypeError):
                print(f"    {value}")


def main():
    args = parse_args()
    set_source(args.source)
    symbols = args.symbols or ["AAPL", "TSLA"]
    interval = args.interval

    print("=" * 60)
    mode_desc = f"数据源: {args.source}"
    if interval > 0:
        mode_desc += f", 持续监控间隔: {interval}秒"
    print(f"  Demo 2: Workflow Agent — 多步骤股票交易决策 [{mode_desc}]")
    print("=" * 60)

    if interval <= 0:
        _run_once(symbols)
        return

    print(f"  持续监控模式已启动，每 {interval} 秒刷新一次 (Ctrl+C 停止)")
    round_num = 0
    try:
        while True:
            round_num += 1
            clear_cache()
            _run_once(symbols, round_num)
            print(f"\n  ⏳ 等待 {interval} 秒后进行下一轮分析...")
            time.sleep(interval)
    except KeyboardInterrupt:
        print(f"\n\n  监控已停止，共完成 {round_num} 轮分析。")


if __name__ == "__main__":
    main()
