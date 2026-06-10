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

---

## Prerequisites

**Required accounts & credentials:**

- [![Sign up for Confluent Cloud](https://img.shields.io/badge/Sign%20up%20for%20Confluent%20Cloud-007BFF?style=for-the-badge&logo=apachekafka&logoColor=white)](https://www.confluent.io/get-started/?utm_campaign=tm.pmm_cd.q4fy25-quickstart-streaming-agents&utm_source=github&utm_medium=demo)
- **LLM Provider:** AWS Bedrock API keys **OR** Azure OpenAI keys - or BYOK

**Required tools:**

- **[Confluent CLI](https://docs.confluent.io/confluent-cli/current/overview.html)** - must be logged in
- **[Docker](https://github.com/docker)** - for Lab1 data generation only
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

**Clone the repository:**

```bash
git clone https://github.com/BigIndustries/quickstart-streaming-agents.git
cd quickstart-streaming-agents
```

---

<details>
<summary><strong>🔧 Setup for Workshop Organizer or For Independent Workshops</strong></summary>

<br>

**Deploy everything in one step:**

```bash
uv run setup
```

The setup wizard guides you through cloud provider selection, credentials, and deploys your chosen lab(s).

See [Workshop Mode Setup Guide](./assets/pre-setup/Workshop-Mode-Setup.md) for the full organizer checklist.

</details>

---

<details open>
<summary><strong>🏫 Setup for Workshop Participants (For Workshops sharing common cloud resources)</strong></summary>

<br>

The workshop uses a single shared Confluent Cloud account. The organizer provisions the shared environment once; each participant then creates their own namespaced resources.

**Organizer** (run once before the workshop):

```bash
uv run setup
```

**Each participant** (run individually during the workshop):

```bash
uv run user
```

> `uv run user` creates a personal service account, API keys, Kafka ACLs, and Flink tables — all namespaced under a prefix derived from the participant's Confluent Cloud email. The organizer must complete `uv run setup` before any participant can run `uv run user`.

</details>

---

## Directory Structure

```
quickstart-streaming-agents/
├── terraform/                          
│   ├── core/                           # Shared Confluent Cloud infra for all labs
│   ├── lab1-tool-calling/              # Lab1-specific infra
│   ├── lab2-vector-search/             # Lab2-specific infra
├── deploy.py                           # Organizer: uv run setup
├── workshop.py                         # Participant: uv run user
└── scripts/                            # Python utilities invoked with uv
```

## Cleanup

```bash
# Automated
uv run destroy
```
