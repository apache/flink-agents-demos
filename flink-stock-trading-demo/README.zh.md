# 智能股票交易 Agent 演示

基于 [Apache Flink Agents](https://github.com/apache/flink-agents) Python SDK 构建的智能股票交易分析系统，通过三个渐进式 Demo 展示框架的核心能力。支持模拟数据和 AKShare 真实行情两种数据源。

## 系列文章

本项目配套四篇深度技术文章，从原理到实战完整覆盖 Flink Agents 的使用方法：

1. **[Apache Flink Agents 框架原理深度解析](article-1-principles.zh.md)** — 三层架构、事件驱动模型、资源延迟实例化、三级记忆、工具元数据提取、编译执行流程，以及与 LangChain/CrewAI/AutoGen 等框架的深度对比
2. **[用 60 行代码构建智能股票分析 Agent — ReAct 模式实战](article-2-react-agent.zh.md)** — ReAct 推理循环、工具系统、结构化输出、真实行情接入
3. **[构建多阶段股票交易决策系统 — Workflow 模式实战](article-3-workflow-agent.zh.md)** — 双模型编排、感知/短期记忆、Skills 技能系统、数据源抽象层
4. **[零 Python 资源定义 — YAML 声明式配置实战](article-4-yaml-agent.zh.md)** — 别名系统、函数引用、YAML 与 Python 混合开发

## 环境准备

### 前置条件

- Python 3.10 / 3.11 / 3.12
- 通义千问 API Key（[DashScope 控制台](https://dashscope.console.aliyun.com/)）

### 安装

```bash
# 1. 安装 Flink Agents（从源码）
cd /path/to/flink-agents
pip install -e python/

# 2. 安装额外依赖
cd stock-trading-demo/
pip install -r requirements.txt

# 3. 设置 API Key
export DASHSCOPE_API_KEY=your_api_key_here
```

## 运行演示

每个 Demo 都支持 `--source` 和 `--symbols` 两个命令行参数：

- `--source demo`（默认）：使用模拟数据，无需网络
- `--source real`：通过 AKShare 获取美股真实行情
- `--symbols`：指定股票代码，如 `AAPL TSLA NVDA`

### Demo 1: ReAct Agent（最简 — 约 60 行核心代码）

```bash
python react_agent_demo.py                                  # 模拟数据
python react_agent_demo.py --source real --symbols AAPL     # 美股真实行情
```

LLM 自主决定调用哪些工具（RSI、MACD、新闻），输出结构化的 `TradingDecision` JSON。

### Demo 2: Workflow Agent（综合 — 展示全部框架能力）

```bash
python workflow_agent_demo.py                                # 模拟数据
python workflow_agent_demo.py --source real --symbols NVDA   # 美股真实行情
```

两阶段处理链：技术分析 → 交易决策，展示装饰器、记忆系统、Skills 等。

### Demo 3: YAML Agent（声明式 — 零 Python 资源定义）

```bash
python yaml_agent_demo.py                                    # 模拟数据
python yaml_agent_demo.py --source real --symbols GOOGL      # 美股真实行情
```

同样的逻辑通过 `stock_analyst.yaml` 声明式配置。

## 数据源架构

```
data_source.py  ←── 代理模块，统一接口
    ├── mock_data.py   ←── 确定性随机模拟数据（默认）
    └── real_data.py   ←── AKShare 美股真实行情
         ├── 实时/历史日线: stock_us_daily（Sina 源）
         └── 股票新闻: stock_news_em
```

## 框架能力覆盖

| 能力 | Demo 1 | Demo 2 | Demo 3 |
|------|:------:|:------:|:------:|
| ReAct Agent 模式 | ✓ | | |
| Workflow Agent 模式 | | ✓ | |
| @tool 工具系统 | ✓ | ✓ | ✓ |
| @prompt 提示词 | ✓ | ✓ | ✓ |
| @chat_model_setup | | ✓ | ✓ |
| 短期记忆 (short_term_memory) | | ✓ | ✓ |
| 结构化输出 (output_schema) | ✓ | | |
| YAML 声明式配置 | | | ✓ |
| Skills 技能系统 | | ✓ | |
| 多步骤事件链 | | ✓ | ✓ |
| 真实行情数据 | ✓ | ✓ | ✓ |

## 项目结构

```
stock-trading-demo/
├── config.py              # LLM 配置
├── models.py              # 数据模型（StockTick, TradingDecision）
├── tools.py               # 工具函数（RSI, MACD, 新闻, 持仓, 下单）
├── prompts.py             # Prompt 模板
├── data_source.py         # 数据源代理（--source 切换）
├── mock_data.py           # 模拟行情数据
├── real_data.py           # AKShare 真实行情
├── react_agent_demo.py    # Demo 1: ReAct Agent
├── workflow_agent_demo.py # Demo 2: Workflow Agent
├── yaml_agent_demo.py     # Demo 3: YAML Agent
├── stock_analyst.yaml     # YAML Agent 定义
├── requirements.txt       # Python 依赖
├── article-1-principles.zh.md    # 文章1: 框架原理
├── article-2-react-agent.zh.md   # 文章2: ReAct 实战
├── article-3-workflow-agent.zh.md # 文章3: Workflow 实战
├── article-4-yaml-agent.zh.md    # 文章4: YAML 实战
└── skills/
    └── market-screener/
        ├── SKILL.md       # 市场筛选技能（英文）
        └── SKILL.zh.md    # 市场筛选技能（中文）
```

## 切换 LLM

修改 `config.py` 中的配置，并替换对应的 `ResourceName.ChatModel.*` 即可切换到 OpenAI / Ollama / Anthropic 等其他模型。

## 扩展到 Flink 分布式运行

将 `from_list()` 替换为 `from_datastream()`，从 Kafka 读取实时行情流：

```python
from pyflink.datastream import StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment()
agents_env = AgentsExecutionEnvironment.get_execution_environment(env)
# ... 从 Kafka source 读取行情 ...
result_stream = agents_env.from_datastream(input=tick_stream, key_selector=lambda x: x.symbol).apply(agent).to_datastream()
agents_env.execute("Stock Trading Job")
```
