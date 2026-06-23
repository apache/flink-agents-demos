"""Demo 1: ReAct Agent — 最简股票分析 Agent。

展示能力:
  - ReAct Agent 模式（LLM 自主决定调用哪些工具）
  - 工具注册（Tool.from_callable）
  - 结构化输出（output_schema -> TradingDecision）

运行:
  export DASHSCOPE_API_KEY=your_key
  python react_agent_demo.py                          # 模拟数据（默认）
  python react_agent_demo.py --source real             # AKShare 真实行情
  python react_agent_demo.py --source real --symbols AAPL NVDA
"""

from flink_agents.api.agents.react_agent import ReActAgent
from flink_agents.api.execution_environment import AgentsExecutionEnvironment
from flink_agents.api.resource import ResourceDescriptor, ResourceName, ResourceType
from flink_agents.api.tools.tool import Tool

from config import TONGYI_CONNECTION_NAME, TONGYI_MODEL, TONGYI_TEMPERATURE
from data_source import generate_stock_ticks, get_price_history_json, parse_args, set_source
from models import TradingDecision
from prompts import react_agent_prompt
from tools import calculate_macd, calculate_rsi, get_stock_news


def main():
    args = parse_args()
    set_source(args.source)

    print("=" * 60)
    print(f"  Demo 1: ReAct Agent — 智能股票分析 [数据源: {args.source}]")
    print("=" * 60)

    # ---- 1. 创建执行环境 ----
    env = AgentsExecutionEnvironment.get_execution_environment()

    # ---- 2. 注册通义千问连接 ----
    env.add_resource(
        TONGYI_CONNECTION_NAME,
        ResourceType.CHAT_MODEL_CONNECTION,
        ResourceDescriptor(clazz=ResourceName.ChatModel.TONGYI_CONNECTION),
    )

    # ---- 3. 注册工具 ----
    env.add_resource(
        "calculate_rsi", ResourceType.TOOL, Tool.from_callable(calculate_rsi)
    )
    env.add_resource(
        "calculate_macd", ResourceType.TOOL, Tool.from_callable(calculate_macd)
    )
    env.add_resource(
        "get_stock_news", ResourceType.TOOL, Tool.from_callable(get_stock_news)
    )

    # ---- 4. 创建 ReAct Agent ----
    # ReActAgent 自动处理: InputEvent → ChatRequest → Tool 调用循环 → OutputEvent
    agent = ReActAgent(
        chat_model=ResourceDescriptor(
            clazz=ResourceName.ChatModel.TONGYI_SETUP,
            connection=TONGYI_CONNECTION_NAME,
            model=TONGYI_MODEL,
            temperature=TONGYI_TEMPERATURE,
            tools=["calculate_rsi", "calculate_macd", "get_stock_news"],
        ),
        prompt=react_agent_prompt,
        output_schema=TradingDecision,
    )

    # ---- 5. 生成行情数据 ----
    symbols = args.symbols or ["AAPL", "TSLA"]
    input_list = generate_stock_ticks(symbols, num_ticks=1)
    for item in input_list:
        item["value"].price_history = get_price_history_json(item["key"])
    print(f"\n输入 {len(input_list)} 条行情数据:")
    for item in input_list:
        tick = item["value"]
        print(f"  [{item['key']}] 价格={tick.price} 成交量={tick.volume}")

    # ---- 6. 执行 ----
    output_list = env.from_list(input_list).apply(agent).to_list()
    env.execute()

    # ---- 7. 输出结果 ----
    print(f"\n{'=' * 60}")
    print("  交易决策结果:")
    print("=" * 60)
    for output in output_list:
        for key, value in output.items():
            print(f"\n  [{key}] {value}")


if __name__ == "__main__":
    main()
