# Apache Flink Agents Demos

A collection of demo projects and examples for [Apache Flink Agents](https://github.com/apache/flink-agents) — an Agentic AI framework based on Apache Flink.

## About

This repository hosts community-contributed demo projects that showcase various use cases and capabilities of Apache Flink Agents. **Anyone is welcome to contribute their demo code** — whether it's a simple getting-started example, an advanced multi-agent workflow, or an integration with external systems.

## Repository Structure

Each demo lives in its own top-level directory:

```
flink-agents-demos/
├── demo-a/          # Each demo is a self-contained project
│   ├── README.md    # Description, setup instructions, and usage
│   └── ...
├── demo-b/
│   ├── README.md
│   └── ...
└── ...
```

## Contributing a Demo

We welcome contributions from everyone! To add your demo:

1. **Fork** this repository and create a new branch.
2. **Create a new directory** at the root level with a descriptive name for your demo (e.g., `chatbot-with-tools/`, `multi-agent-rag/`).
3. **Include a `README.md`** in your demo directory that covers:
   - What the demo does
   - Prerequisites
   - How to set up and run it
   - Expected output or screenshots (if applicable)
4. **Keep it self-contained** — each demo should be independently runnable without dependencies on other demos in this repo.
5. **Submit a Pull Request** with a clear description of your demo.

### Guidelines

- Make sure your demo works with a released or recent version of [Flink Agents](https://github.com/apache/flink-agents).
- Use clear, readable code with appropriate comments.
- Avoid committing secrets, API keys, or other sensitive information. Use environment variables or configuration files (added to `.gitignore`) instead.
- You may use any programming language supported by Flink Agents (Java, Python, etc.).

## Community

- **Slack**: Join the [Apache Flink Slack workspace](https://flink.apache.org/what-is-flink/community/#slack) and find us in [#flink-agents-user](https://apache-flink.slack.com/archives/C09KP5YUWE8) and [#flink-agents-dev](https://apache-flink.slack.com/archives/C097QF5HG8J).
- **Flink Agents Repository**: [https://github.com/apache/flink-agents](https://github.com/apache/flink-agents)

## License

This project is licensed under the [Apache License 2.0](LICENSE).
