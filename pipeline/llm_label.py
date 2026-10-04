"""BONUS — an LLM inside the pipeline (slide "LLM là một bước transform").

The support team wants an LLM pre-triage label on every live ticket
(gold_ticket_labels), to compare with the human `category` and to triage new
tickets faster. An LLM step is a transform like any other — except it is
expensive, slow and NOT deterministic, so the slide's four rules apply:

  1. key = hash(input) + model + prompt version  -> a re-run makes 0 LLM calls;
     changing the prompt re-labels everything ON PURPOSE
  2. force a structured output, validate it; invalid -> quarantine, never Gold
  3. estimate the cost BEFORE running (rows x tokens x price)
  4. LLM labels are versioned data (model + prompt_version stored on every row)

The shipped `label_tickets` is the NAIVE version: it calls the model for every
ticket on every run and writes whatever comes back. Your bonus task is to make
`python -m scripts.bonus_llm` print BONUS PASS. Zero-key: `FakeLLM` stands in for a
real model (swap in any provider via .env if you like — the pipeline is the same).
"""
from __future__ import annotations

import hashlib
import json
import re

import duckdb

MODEL = "fake-llm-2026-09"
PROMPT_VERSION = "triage-v1"
ALLOWED_LABELS = ("bug", "billing", "other")
PRICE_PER_1K_TOKENS_USD = 0.002          # pretend price, for the cost estimate


PROMPT_TEMPLATE = """You triage customer-support tickets.
Answer ONLY with JSON: {{"label": "bug" | "billing" | "other"}}.
Ticket: {text}"""


class FakeLLM:
    """Deterministic stand-in for a chat model. Counts calls and tokens."""

    def __init__(self, model: str = MODEL) -> None:
        self.model = model
        self.calls = 0
        self.tokens = 0

    def complete(self, prompt: str) -> str:
        self.calls += 1
        self.tokens += len(prompt.split()) + 8
        text = prompt.lower()
        if "xuất" in text:
            return 'Sure! Here is the label: {"label": "export"}'   # off-schema answer
        if re.search(r"crash|lỗi|sso|đăng nhập|chatbot", text):
            return '{"label": "bug"}'
        if re.search(r"tiền|hoá đơn|thanh toán|gói|vat", text):
            return '{"label": "billing"}'
        return '{"label": "other"}'


def estimate_tokens(texts: list[str]) -> int:
    return sum(len(PROMPT_TEMPLATE.format(text=t).split()) + 8 for t in texts)


def parse_label(raw: str) -> str | None:
    """Pull {"label": ...} out of the model's answer; None if it is not valid."""
    m = re.search(r"\{.*\}", raw, flags=re.S)
    if not m:
        return None
    try:
        label = json.loads(m.group(0)).get("label")
    except json.JSONDecodeError:
        return None
    return label if label in ALLOWED_LABELS else None


def live_tickets(con: duckdb.DuckDBPyConnection) -> list[tuple[str, str]]:
    return con.execute("""
        SELECT ticket_id, subject || '. ' || body AS text
        FROM silver_tickets
        WHERE NOT is_deleted
        ORDER BY ticket_id
    """).fetchall()


def label_tickets(con: duckdb.DuckDBPyConnection, llm: FakeLLM) -> dict:
    con.execute("""CREATE TABLE IF NOT EXISTS llm_cache (
        cache_key VARCHAR PRIMARY KEY,
        raw_answer VARCHAR)""")
    con.execute("""CREATE TABLE IF NOT EXISTS llm_label_quarantine (
        ticket_id VARCHAR, raw_answer VARCHAR, model VARCHAR, prompt_version VARCHAR, reason VARCHAR)""")

    gold_rows = []
    quarantine_rows = []

    for ticket_id, text in live_tickets(con):
        ckey = hashlib.sha256(f"{text}:{llm.model}:{PROMPT_VERSION}".encode("utf-8")).hexdigest()
        cached = con.execute("SELECT raw_answer FROM llm_cache WHERE cache_key = ?", [ckey]).fetchone()
        if cached:
            raw = cached[0]
        else:
            raw = llm.complete(PROMPT_TEMPLATE.format(text=text))
            con.execute("INSERT INTO llm_cache VALUES (?, ?)", [ckey, raw])

        label = parse_label(raw)
        if label is not None:
            gold_rows.append((ticket_id, label, llm.model, PROMPT_VERSION))
        else:
            quarantine_rows.append((ticket_id, raw, llm.model, PROMPT_VERSION, "invalid or off-schema"))

    con.execute("""CREATE OR REPLACE TABLE gold_ticket_labels (
        ticket_id VARCHAR, label VARCHAR, model VARCHAR, prompt_version VARCHAR)""")
    if gold_rows:
        con.executemany("INSERT INTO gold_ticket_labels VALUES (?, ?, ?, ?)", gold_rows)

    con.execute("DELETE FROM llm_label_quarantine WHERE prompt_version = ?", [PROMPT_VERSION])
    if quarantine_rows:
        con.executemany("INSERT INTO llm_label_quarantine VALUES (?, ?, ?, ?, ?)", quarantine_rows)

    return {"labeled": len(gold_rows), "quarantined": len(quarantine_rows), "calls": llm.calls}
