# Intelligent Stock Trading Agent Demo

An intelligent stock trading analysis system built on [Apache Flink Agents](https://github.com/apache/flink-agents) Python SDK, demonstrating the framework's core capabilities through three progressive demos. Supports both simulated data and AKShare real market data as data sources.

## Series Articles

This project includes four in-depth technical articles, covering Flink Agents from theory to practice:

1. **[Apache Flink Agents Framework: Principles Deep Dive](article-1-principles.md)** — Three-layer architecture, event-driven model, lazy resource instantiation, three-tier memory, tool metadata extraction, compile-and-execute pipeline, and in-depth comparison with LangChain/CrewAI/AutoGen
2. **[Building an Intelligent Stock Analysis Agent in 60 Lines — ReAct Pattern in Practice](article-2-react-agent.md)** — ReAct reasoning loop, tool system, structured output, real market data integration
3. **[Building a Multi-Stage Stock Trading Decision System — Workflow Pattern in Practice](article-3-workflow-agent.md)** — Dual-model orchestration, perception/short-term memory, Skills system, data source abstraction layer
4. **[Zero-Python Resource Definition — YAML Declarative Configuration in Practice](article-4-yaml-agent.md)** — Alias system, function references, YAML and Python hybrid development

## Prerequisites

### Requirements

- Python 3.10 / 3.11 / 3.12
- Tongyi Qianwen (Qwen) API Key ([DashScope Console](https://dashscope.console.aliyun.com/))

### Installation

```bash
# 1. Install Flink Agents (from source)
cd /path/to/flink-agents
pip install -e python/

# 2. Install additional dependencies
cd stock-trading-demo/
pip install -r requirements.txt

# 3. Set API Key
export DASHSCOPE_API_KEY=your_api_key_here
```

## Running the Demos

Each demo supports `--source` and `--symbols` command-line arguments:

- `--source demo` (default): uses simulated data, no network required
- `--source real`: fetches real US stock market data via AKShare
- `--symbols`: specify stock tickers, e.g. `AAPL TSLA NVDA`

### Demo 1: ReAct Agent (Minimal — ~60 lines of core code)

```bash
python react_agent_demo.py                                  # Simulated data
python react_agent_demo.py --source real --symbols AAPL     # Real US stock data
```

The LLM autonomously decides which tools to call (RSI, MACD, News) and outputs a structured `TradingDecision` JSON.

### Demo 2: Workflow Agent (Comprehensive — showcases all framework capabilities)

```bash
python workflow_agent_demo.py                                # Simulated data
python workflow_agent_demo.py --source real --symbols NVDA   # Real US stock data
```

Two-stage processing chain: Technical Analysis → Trading Decision, demonstrating decorators, memory system, Skills, etc.

### Demo 3: YAML Agent (Declarative — zero Python resource definitions)

```bash
python yaml_agent_demo.py                                    # Simulated data
python yaml_agent_demo.py --source real --symbols GOOGL      # Real US stock data
```

The same logic defined declaratively via `stock_analyst.yaml`.

## Data Source Architecture

```
data_source.py  ←── Proxy module, unified interface
    ├── mock_data.py   ←── Deterministic random simulated data (default)
    └── real_data.py   ←── AKShare US stock real market data
         ├── Real-time/Historical Daily: stock_us_daily (Sina source)
         └── Stock News: stock_news_em
```

## Framework Capability Coverage

| Capability | Demo 1 | Demo 2 | Demo 3 |
|------------|:------:|:------:|:------:|
| ReAct Agent Pattern | ✓ | | |
| Workflow Agent Pattern | | ✓ | |
| @tool System | ✓ | ✓ | ✓ |
| @prompt Prompting | ✓ | ✓ | ✓ |
| @chat_model_setup | | ✓ | ✓ |
| Short-term Memory (short_term_memory) | | ✓ | ✓ |
| Structured Output (output_schema) | ✓ | | |
| YAML Declarative Configuration | | | ✓ |
| Skills System | | ✓ | |
| Multi-step Event Chain | | ✓ | ✓ |
| Real Market Data | ✓ | ✓ | ✓ |

## Project Structure

```
stock-trading-demo/
├── config.py              # LLM Configuration
├── models.py              # Data Models (StockTick, TradingDecision)
├── tools.py               # Tool Functions (RSI, MACD, News, Positions, Orders)
├── prompts.py             # Prompt Templates
├── data_source.py         # Data Source Proxy (--source switch)
├── mock_data.py           # Simulated Market Data
├── real_data.py           # AKShare Real Market Data
├── react_agent_demo.py    # Demo 1: ReAct Agent
├── workflow_agent_demo.py # Demo 2: Workflow Agent
├── yaml_agent_demo.py     # Demo 3: YAML Agent
├── stock_analyst.yaml     # YAML Agent Definition
├── requirements.txt       # Python Dependencies
├── article-1-principles.md    # Article 1: Framework Principles
├── article-2-react-agent.md   # Article 2: ReAct in Practice
├── article-3-workflow-agent.md # Article 3: Workflow in Practice
├── article-4-yaml-agent.md    # Article 4: YAML in Practice
└── skills/
    └── market-screener/
        └── SKILL.md       # Market Screener Skill
```

## Switching LLMs

Modify the configuration in `config.py` and replace the corresponding `ResourceName.ChatModel.*` to switch to other models such as OpenAI, Ollama, Anthropic, etc.

## Extending to Flink Distributed Execution

Replace `from_list()` with `from_datastream()` to read real-time market data streams from Kafka:

```python
from pyflink.datastream import StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment()
agents_env = AgentsExecutionEnvironment.get_execution_environment(env)
# ... read market data from Kafka source ...
result_stream = agents_env.from_datastream(input=tick_stream, key_selector=lambda x: x.symbol).apply(agent).to_datastream()
agents_env.execute("Stock Trading Job")
```