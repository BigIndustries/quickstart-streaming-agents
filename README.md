# Streaming Agents on Confluent Cloud Quickstart

[![Sign up for Confluent Cloud](https://img.shields.io/badge/Sign%20up%20for%20Confluent%20Cloud-007BFF?style=for-the-badge&logo=apachekafka&logoColor=white)](https://www.confluent.io/get-started/?utm_campaign=tm.pmm_cd.q4fy25-quickstart-streaming-agents&utm_source=github&utm_medium=demo)

<div align="center">
  <a href="https://www.youtube.com/watch?v=3fWMD3qqBR8">
    <img src="https://img.youtube.com/vi/3fWMD3qqBR8/maxresdefault.jpg" alt="Watch Demo Video" style="width:100%;max-width:800px;">
  </a>
</div>

Build real-time AI agents with [Confluent Cloud Streaming Agents](https://docs.confluent.io/cloud/current/ai/streaming-agents/overview.html). This quickstart includes three hands-on labs:

<table>
<tr>
<th width="25%">Lab</th>
<th width="75%">Description</th>
</tr>
<tr>
<td><a href="./LAB1-Walkthrough.md"><strong>Lab1 - Price Matching Orders With MCP Tool Calling</strong></a></td>
<td><b>*NEW!*</b> Now using new Agent Definition (CREATE AGENT) syntax. Price matching agent that scrapes competitor websites and adjusts prices in real-time.<br><br><img src="./assets/lab1/lab1-architecture.png" alt="Lab1 architecture diagram"></td>
</tr>
<tr>
<td><a href="./LAB2-Walkthrough.md"><strong>Lab2 - Vector Search & RAG</strong></a></td>
<td>Vector search pipeline template with retrieval augmented generation (RAG). Use the included Flink documentation chunks, or bring your own documents for intelligent document retrieval.<br><br><img src="./assets/lab2/00_lab2_architecture.png" alt="Lab2 architecture diagram"></td>
</tr>
</table>

## Prerequisites

**Required accounts & credentials:**

- [![Sign up for Confluent Cloud](https://img.shields.io/badge/Sign%20up%20for%20Confluent%20Cloud-007BFF?style=for-the-badge&logo=apachekafka&logoColor=white)](https://www.confluent.io/get-started/?utm_campaign=tm.pmm_cd.q4fy25-quickstart-streaming-agents&utm_source=github&utm_medium=demo)
- **LLM Provider:** AWS Bedrock API keys **OR** Azure OpenAI keys - or BYOK

**Required tools:**

- **[Confluent CLI](https://docs.confluent.io/confluent-cli/current/overview.html)** - must be logged in
- **[Docker](https://github.com/docker)** - for Lab1 & Lab3 data generation only
- **[Git](https://github.com/git/git)**
- **[Terraform](https://github.com/hashicorp/terraform)**
- **[uv](https://github.com/astral-sh/uv)**
- **[AWS CLI](https://github.com/aws/aws-cli)** or **[Azure CLI](https://github.com/Azure/azure-cli)** tools for generating API keys

<details>
<summary> Installation commands (Mac/Windows)</summary>
**Mac:**

```bash
brew install uv git python && brew tap hashicorp/tap && brew install hashicorp/tap/terraform && brew install --cask confluent-cli docker-desktop && brew install awscli # or azure-cli
```

**Windows:**

```powershell
winget install astral-sh.uv Git.Git Docker.DockerDesktop Hashicorp.Terraform ConfluentInc.Confluent-CLI Python.Python
```
</details>

## 🚀 Quick Start

**1. Clone the repository and navigate to the Quickstart directory:**

```bash
git clone https://github.com/BigIndustries/quickstart-streaming-agents.git
cd quickstart-streaming-agents
```

2. **One command deployment:**

```bash
uv run deploy
```

That's it! The script will guide you through the setup and deployment of your chosen lab(s).

## Directory Structure

```
quickstart-streaming-agents/
├── terraform/                          
│   ├── core/                           # Shared Confluent Cloud infra for all labs
│   ├── lab1-tool-calling/              # Lab1-specific infra
│   ├── lab2-vector-search/             # Lab2-specific infra
├── deploy.py                           # Start here with uv run deploy
└── scripts/                            # Python utilities invoked with uv
```

## Cleanup

```bash
# Automated
uv run destroy
```