"""Demo 3: YAML Agent — 声明式股票分析 Agent。

展示能力:
  - YAML 声明式 Agent 配置（无需 Python 类定义资源）
  - load_yaml + apply("agent_name") 模式

运行:
  export DASHSCOPE_API_KEY=your_key
  python yaml_agent_demo.py                          # 模拟数据（默认）
  python yaml_agent_demo.py --source real             # AKShare 真实行情
  python yaml_agent_demo.py --source real --symbols GOOGL
"""

import json
import sys
from pathlib import Path

from flink_agents.api.execution_environment import AgentsExecutionEnvironment

from data_source import generate_stock_ticks, parse_args, set_source

# 确保当前目录在 sys.path 中（YAML 中的 function 引用需要）
current_dir = str(Path(__file__).parent)
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)


def main():
    args = parse_args()
    set_source(args.source)

    print("=" * 60)
    print(f"  Demo 3: YAML Agent — 声明式股票分析 [数据源: {args.source}]")
    print("=" * 60)

    # ---- 创建执行环境 ----
    env = AgentsExecutionEnvironment.get_execution_environment()

    # ---- 加载 YAML Agent 定义 ----
    yaml_path = str(Path(__file__).parent / "stock_analyst.yaml")
    env.load_yaml(yaml_path)

    # ---- 生成行情数据 ----
    symbols = args.symbols or ["GOOGL"]
    input_list = generate_stock_ticks(symbols, num_ticks=1)
    print(f"\n输入 {len(input_list)} 条行情数据:")
    for item in input_list:
        tick = item["value"]
        print(f"  [{item['key']}] 价格={tick.price} 成交量={tick.volume}")

    # ---- 用 Agent 名称引用并执行 ----
    output_list = env.from_list(input_list).apply("stock_analyst").to_list()
    env.execute()

    # ---- 输出 ----
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


if __name__ == "__main__":
    main()
