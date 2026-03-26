"""
Tests for executor.py — all offline.
"""

import json
import os
import time
import pytest
from pathlib import Path
from unittest.mock import patch

from arb_detector import ArbOpportunity
from executor import should_execute, log_opportunity


def _make_opp(profitable=True, profit=31.14, pair="cbBTC/USDC"):
    return ArbOpportunity(
        pair=pair,
        buy_venue="uniswap",
        sell_venue="aerodrome",
        buy_price=68193.69,
        sell_price=68297.04,
        gross_spread_pct=0.1516,
        total_fee_pct=0.06,
        net_spread_pct=0.0916,
        flash_loan_usdc=34000.0,
        estimated_profit_usdc=profit,
        is_profitable=profitable,
        timestamp=time.time(),
    )


# ── should_execute ─────────────────────────────────────────────────────────────

def test_should_execute_false_when_execute_mode_off():
    """EXECUTE_MODE=false → should_execute returns False."""
    opp = _make_opp(profitable=True, profit=50.0)
    with patch("executor.config.EXECUTE_MODE", False):
        result, reason = should_execute(opp)
    assert result is False
    assert "EXECUTE_MODE=false" in reason


def test_should_execute_false_when_no_contract_address():
    """No ARB_EXECUTOR_ADDRESS → should_execute returns False."""
    opp = _make_opp(profitable=True, profit=50.0)
    with patch("executor.config.EXECUTE_MODE", True), \
         patch("executor.config.ARB_EXECUTOR_ADDRESS", ""):
        result, reason = should_execute(opp)
    assert result is False
    assert "ARB_EXECUTOR_ADDRESS" in reason


def test_should_execute_false_when_below_min_profit():
    """Profit below MIN_NET_PROFIT_USD → should_execute returns False."""
    opp = _make_opp(profitable=True, profit=2.0)
    with patch("executor.config.EXECUTE_MODE", True), \
         patch("executor.config.ARB_EXECUTOR_ADDRESS", "0xDEAD"), \
         patch("executor.config.MIN_NET_PROFIT_USD", 10.0):
        result, reason = should_execute(opp)
    assert result is False
    assert "profit" in reason.lower() or "$2.00" in reason


def test_should_execute_false_when_not_profitable():
    """is_profitable=False → should_execute returns False regardless of other flags."""
    opp = _make_opp(profitable=False, profit=50.0)
    with patch("executor.config.EXECUTE_MODE", True), \
         patch("executor.config.ARB_EXECUTOR_ADDRESS", "0xDEAD"):
        result, reason = should_execute(opp)
    assert result is False
    assert "profitable" in reason.lower()


def test_should_execute_true_when_all_gates_pass():
    """All gates pass → should_execute returns (True, "")."""
    opp = _make_opp(profitable=True, profit=50.0)
    with patch("executor.config.EXECUTE_MODE", True), \
         patch("executor.config.ARB_EXECUTOR_ADDRESS", "0xDEAD1234"), \
         patch("executor.config.MIN_NET_PROFIT_USD", 10.0):
        result, reason = should_execute(opp)
    assert result is True
    assert reason == ""


# ── log_opportunity ────────────────────────────────────────────────────────────

def test_log_opportunity_writes_jsonl(tmp_path):
    """log_opportunity must write to executions.jsonl."""
    opp = _make_opp()
    with patch("executor.config.LOG_DIR", str(tmp_path)):
        log_opportunity(opp, "PROFITABLE")

    jsonl_path = tmp_path / "executions.jsonl"
    assert jsonl_path.exists(), "executions.jsonl not created"
    lines = jsonl_path.read_text().strip().split("\n")
    assert len(lines) == 1


def test_log_opportunity_all_fields_present(tmp_path):
    """Every required field must be present in the logged JSONL record."""
    opp = _make_opp()
    with patch("executor.config.LOG_DIR", str(tmp_path)):
        log_opportunity(opp, "PROFITABLE")

    jsonl_path = tmp_path / "executions.jsonl"
    record = json.loads(jsonl_path.read_text().strip())

    required = [
        "timestamp", "tag", "pair", "buy_venue", "sell_venue",
        "buy_price", "sell_price", "gross_spread_pct", "net_spread_pct",
        "flash_loan_usdc", "estimated_profit_usdc", "tx_hash",
        "actual_profit_usdc", "error",
    ]
    for field in required:
        assert field in record, f"Missing field: {field}"


def test_jsonl_parseable(tmp_path):
    """Multiple logged records must all be valid JSON lines."""
    opp = _make_opp()
    tags = ["PROFITABLE", "DRY", "SKIP", "BELOW_THRESHOLD"]
    with patch("executor.config.LOG_DIR", str(tmp_path)):
        for tag in tags:
            log_opportunity(opp, tag)

    jsonl_path = tmp_path / "executions.jsonl"
    lines = jsonl_path.read_text().strip().split("\n")
    assert len(lines) == len(tags)
    for i, line in enumerate(lines):
        record = json.loads(line)
        assert record["tag"] == tags[i]


def test_log_opportunity_tag_stored(tmp_path):
    """The tag in the JSONL record must match what was passed."""
    opp = _make_opp()
    for tag in ("PROFITABLE", "DRY", "SKIP", "ERROR", "BELOW_THRESHOLD"):
        with patch("executor.config.LOG_DIR", str(tmp_path)):
            log_opportunity(opp, tag)

    jsonl_path = tmp_path / "executions.jsonl"
    records = [json.loads(l) for l in jsonl_path.read_text().strip().split("\n")]
    stored_tags = [r["tag"] for r in records]
    assert "PROFITABLE" in stored_tags
    assert "DRY" in stored_tags
