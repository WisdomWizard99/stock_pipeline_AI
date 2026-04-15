import logging
import ollama
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import OLLAMA_MODEL
from ai_insights.query_snowflake import (
    fetch_latest_metrics,
    fetch_recent_performance,
    format_metrics_for_llm
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """
You are a senior financial data analyst with deep expertise
in stock market analysis and data engineering pipelines.

You have access to processed stock market data ingested from
Yahoo Finance, cleaned through an ETL pipeline, and aggregated
into key metrics including moving averages, volatility, and
daily returns.

When analyzing data:
- Be specific — reference actual numbers from the data
- Be concise — busy analysts need clear actionable insights
- Highlight the most important finding first
- Flag any risks or concerns clearly
- Use plain English — avoid unnecessary jargon
- Format response with clear numbered sections
"""


def ask_ollama(prompt: str, max_tokens: int = 1024) -> str:
    """
    Sends a prompt to the local Ollama model and returns
    the response as a string.

    Single function that talks to Ollama — all other functions
    call this one. Switching models only requires changing
    OLLAMA_MODEL in config.py — nothing else changes.

    Args:
        prompt:     user prompt to send
        max_tokens: maximum response length

    Returns:
        Model response as plain string
    """
    logger.info(f"Sending prompt to Ollama ({OLLAMA_MODEL})...")

    response = ollama.chat(
        model    = OLLAMA_MODEL,
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": prompt}
        ],
        options  = {"num_predict": max_tokens}
    )

    result = response["message"]["content"]
    logger.info("✓ Response received from Ollama")
    return result


def generate_market_summary() -> str:
    """
    Generates a natural language summary of current
    market conditions across all tracked stocks.

    Returns:
        String containing market analysis
    """
    logger.info("Generating market summary...")

    metrics_df = fetch_latest_metrics()
    context    = format_metrics_for_llm(metrics_df)

    prompt = f"""
Based on the following stock metrics from our data pipeline,
provide a concise market summary covering:

1. Overall market sentiment — are most stocks trending up or down?
2. Best and worst performing stocks by average daily return
3. Which stocks show bullish signals (MA7 > MA30)?
4. Which stocks show high volatility — potential risk?
5. One key actionable insight for each stock

Here is the data:

{context}
"""
    return ask_ollama(prompt, max_tokens=1024)


def answer_question(user_question: str) -> str:
    """
    Answers a natural language question about the stock data.
    This powers the Q&A feature in the Streamlit dashboard.

    Args:
        user_question: plain English question from the user

    Returns:
        Model answer as string
    """
    logger.info(f"Answering: {user_question}")

    metrics_df = fetch_latest_metrics()
    context    = format_metrics_for_llm(metrics_df)

    prompt = f"""
You have access to the following stock market data:

{context}

Answer this question clearly and concisely,
referencing specific numbers from the data where relevant:

Question: {user_question}
"""
    return ask_ollama(prompt, max_tokens=512)


def generate_ticker_deep_dive(ticker: str) -> str:
    """
    Generates a detailed analysis of one specific stock
    using its recent 30-day performance data.

    Args:
        ticker: stock symbol e.g. "AAPL"

    Returns:
        Detailed analysis of that stock
    """
    logger.info(f"Generating deep dive for {ticker}...")

    recent_df = fetch_recent_performance(days=30)
    ticker_df = recent_df[recent_df["TICKER"] == ticker]

    if ticker_df.empty:
        return f"No data found for ticker {ticker}."

    data_str = ticker_df[[
        "DATE", "CLOSE", "DAILY_RETURN_PCT",
        "MA_7", "MA_30", "VOLATILITY_30"
    ]].to_string(index=False)

    prompt = f"""
Here is the last 30 days of data for {ticker}:

{data_str}

Provide a detailed analysis covering:
1. Recent price trend — consistently moving up or down?
2. Momentum — is MA7 above or below MA30?
3. Volatility — how has risk changed over this period?
4. Notable events — any unusual price moves worth flagging?
5. Overall outlook — bullish, bearish, or neutral?
"""
    return ask_ollama(prompt, max_tokens=768)


if __name__ == "__main__":

    print("\n" + "=" * 60)
    print("MARKET SUMMARY")
    print("=" * 60)
    summary = generate_market_summary()
    print(summary)

    print("\n" + "=" * 60)
    print("Q&A — Which stock had the highest return?")
    print("=" * 60)
    answer = answer_question(
        "Which stock had the highest average daily return "
        "and what does that tell us?"
    )
    print(answer)

    print("\n" + "=" * 60)
    print("DEEP DIVE — NVDA")
    print("=" * 60)
    deep_dive = generate_ticker_deep_dive("NVDA")
    print(deep_dive)