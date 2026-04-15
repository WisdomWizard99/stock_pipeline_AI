# Stock Pipeline AI

An end-to-end **production-grade data engineering pipeline** that ingests stock market data, stores it in a cloud data lake, transforms it into business-ready metrics, and uses a **local LLM (Llama 3.2 via Ollama)** to generate natural language insights — all without sending sensitive financial data to external APIs.

---

## 🏗️ Architecture

```
yfinance API
     │
     │  Daily stock prices (OHLCV)
     ▼
Python Ingestion Layer
     │
     │  Fetch → Validate → Upload
     ▼
Azure Blob Storage          ← Bronze Layer (raw, immutable)
raw/2026-04-07_09-00-00.csv
     │
     │  Extract → Transform → Aggregate (PySpark)
     ▼
Snowflake                   ← Silver + Gold Layers
├── STOCK_PRICES_CLEAN       (Silver — cleaned, normalized)
└── STOCK_DAILY_METRICS      (Gold  — KPIs, moving averages)
     │
     │  Query → Format → Prompt
     ▼
Ollama (Llama 3.2 — local)  ← AI Insights Layer
     │
     │  Natural language summaries, Q&A, deep dives
     ▼
Streamlit Dashboard         ← Interactive UI
```

---

## Medallion Architecture

| Layer | Storage | Description |
|-------|---------|-------------|
| **Bronze** | Azure Blob Storage | Raw data exactly as received from Yahoo Finance — immutable, timestamped |
| **Silver** | Snowflake `STOCK_PRICES_CLEAN` | Cleaned, normalized, deduplicated — correct types, no nulls |
| **Gold** | Snowflake `STOCK_DAILY_METRICS` | Business KPIs — moving averages, volatility, daily returns |

---

## Tech Stack

| Layer | Technology |
|-------|------------|
| Ingestion | Python, `yfinance`, `pandas` |
| Cloud Storage | Azure Blob Storage |
| Transformation | Python, `pandas`, PySpark |
| Data Warehouse | Snowflake |
| AI Insights | Ollama, Llama 3.2 (local LLM) |
| Dashboard | Streamlit |
| Config | `python-dotenv` |
| Version Control | Git, GitHub |

---

## Project Structure

```
stock-pipeline-ai/
│
├── ingestion/
│   ├── fetch_stocks.py         # Pull OHLCV data from yfinance
│   └── upload_to_blob.py       # Upload raw CSV to Azure Blob (Bronze)
│
├── transformation/
│   ├── extract.py              # Read raw CSV from Azure Blob
│   ├── transform.py            # Clean + normalize → Silver
│   ├── aggregate_spark.py      # Compute KPIs via PySpark → Gold
│   └── load.py                 # Write Silver + Gold to Snowflake
│
├── ai_insights/
│   ├── query_snowflake.py      # Query Gold table, format for LLM
│   └── insights.py             # Ollama integration — summaries, Q&A
│
├── dashboard/
│   └── app.py                  # Streamlit dashboard
│
├── config/
│   └── config.py               # Centralized config — reads from .env
│
├── tests/
│   └── test_transform.py       # Unit tests for transformation logic
│
├── pipeline.py                 # Master orchestrator — runs full pipeline
├── .env.example                # Environment variable template
├── requirements.txt
└── README.md
```

---

## Setup & Installation

### Prerequisites

- Python 3.10+
- Java 8+ (required for PySpark)
- An [Azure account](https://azure.microsoft.com) with a Storage Account and Container
- A [Snowflake account](https://snowflake.com) (free 30-day trial available)
- [Ollama](https://ollama.com) installed locally

---

### Step 1 — Clone the repository

```bash
git clone https://github.com/yourusername/stock-pipeline-ai.git
cd stock-pipeline-ai
```

---

### Step 2 — Create and activate a virtual environment

```bash
python -m venv venv

# Mac/Linux
source venv/bin/activate

# Windows
venv\Scripts\activate
```

---

### Step 3 — Install Python dependencies

```bash
pip install -r requirements.txt
```

---

### Step 4 — Install and set up Ollama

1. Download Ollama from **[ollama.com](https://ollama.com)** and install it
2. Pull the Llama 3.2 model (one-time download, ~2GB):

```bash
ollama pull llama3.2
```

3. Verify Ollama is running:

```bash
ollama --version
```

---

### Step 5 — Set up Azure Blob Storage

1. Create a **Storage Account** in [Azure Portal](https://portal.azure.com)
2. Create a **Container** named `stock-data`
3. Copy your **Connection String** from Access Keys

---

### Step 6 — Set up Snowflake

Sign up at [snowflake.com](https://snowflake.com) (free trial)

---



---

## Running the Pipeline

### Full pipeline — one command

```bash
python pipeline.py
```
This runs all four steps in sequence:

```
[STEP 1/4] Ingestion    — yfinance → Azure Blob Storage
[STEP 2/4] Extraction   — Azure Blob → pandas DataFrame
[STEP 3/4] Transform    — clean + aggregate via PySpark
[STEP 4/4] Load         — Silver + Gold → Snowflake
```

---

### Run individual steps

```bash
# Ingestion only
python -m ingestion.upload_to_blob

# Transformation only
python -m transformation.load

# AI insights only
python -m ai_insights.insights

# Dashboard only
streamlit run dashboard/app.py
```

---

##  AI Insights

The AI layer queries your Snowflake Gold table and uses **Llama 3.2 running locally via Ollama** to generate:

- **Market Summary** — overall sentiment, best/worst performers, trend signals
- **Natural Language Q&A** — ask questions in plain English about your stocks
- **Ticker Deep Dive** — detailed 30-day analysis for any individual stock

---

##  Project Status

- [x] Week 1 — Ingestion (yfinance → Azure Blob)
- [x] Week 2 — ETL Pipeline (Azure Blob → Snowflake Silver + Gold)
- [x] Week 3 — AI Insights (Ollama + Llama 3.2)
- [ ] Week 4 — Streamlit Dashboard

---

