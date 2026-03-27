"""
Tests for executor.py — all offline.
"""

import json
import os
import time
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from arb_detector import ArbOpportunity, SimResult
from executor import should_execute, log_opportunity


def _make_opp(profitable=True, profit=31.14, pair="cbBTC/USDC",
              tier="MARGINAL"):
    return ArbOpportunity(
        pair=pair,
        buy_venue="Uniswap V3",
        sell_venue="Aerodrome Slipstream",
        buy_price=68193.69,
        sell_price=68297.04,
        gross_spread_pct=0.1516,
        total_fee_pct=0.06,
        net_spread_pct=0.0916,
        flash_loan_usdc=17000.0,
        estimated_profit_usdc=profit,
        is_profitable=profitable,
        timestamp=time.time(),
        tier=tier,
    )


def _make_sim(executable=True, reason="", net_profit=15.0):
    return SimResult(
        buy_dex="Uniswap V3",
        sell_dex="Aerodrome Slipstream",
        token_amount=0.249,
        usdc_in=17000.0,
        usdc_out=17015.5,
        gross_profit_usd=15.5,
        gas_cost_usd=0.5,
        net_profit_usd=net_profit,
        flash_provider="Morpho",
        is_executable=executable,
        rejection_reason=reason,
    )


# ── should_execute ─────────────────────────────────────────────────────────────

def test_should_execute_false_when_execute_mode_off():
    """EXECUTE_MODE=false → should_execute returns False."""
    opp = _make_opp(profitable=True, profit=50.0)
    with patch("executor.config.EXECUTE_MODE", False):
        result, reason = should_execute(opp)
    assert result is False
    assert "blocked_by_execute_mode" in reason


def test_should_execute_false_when_no_contract_address():
    """No ARB_EXECUTOR_ADDRESS → should_execute returns False."""
    opp = _make_opp(profitable=True, profit=50.0)
    with patch("executor.config.EXECUTE_MODE", True), \
         patch("executor.config.ARB_EXECUTOR_ADDRESS", ""):
        result, reason = should_execute(opp)
    assert result is False
    assert "blocked_by_no_contract" in reason


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
    """is_profitable=False → should_execute returns False."""
    opp = _make_opp(profitable=False, profit=50.0)
    with patch("executor.config.EXECUTE_MODE", True), \
         patch("executor.config.ARB_EXECUTOR_ADDRESS", "0xDEAD"):
        result, reason = should_execute(opp)
    assert result is False
    assert "profitable" in reason.lower()


def test_should_execute_true_when_all_gates_pass():
    """All gates pass → should_execute returns (True, '')."""
    opp = _make_opp(profitable=True, profit=50.0)
    sim = _make_sim(executable=True)
    with patch("executor.config.EXECUTE_MODE", True), \
         patch("executor.config.ARB_EXECUTOR_ADDRESS", "0xDEAD1234"), \
         patch("executor.config.MIN_NET_PROFIT_USD", 10.0):
        result, reason = should_execute(opp, sim)
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

    record = json.loads((tmp_path / "executions.jsonl").read_text().strip())

    required = [
        "timestamp", "tag", "pair", "buy_venue", "sell_venue",
        "buy_price", "sell_price", "gross_spread_pct", "net_spread_pct",
        "flash_loan_usdc", "estimated_profit_usdc", "tx_hash",
        "actual_profit_usdc", "error",
    ]
    for f in required:
        assert f in record, f"Missing field: {f}"


def test_jsonl_parseable(tmp_path):
    """Multiple logged records must all be valid JSON lines."""
    opp = _make_opp()
    tags = ["PROFITABLE", "DRY", "SKIP", "BELOW_THRESHOLD"]
    with patch("executor.config.LOG_DIR", str(tmp_path)):
        for tag in tags:
            log_opportunity(opp, tag)

    lines = (tmp_path / "executions.jsonl").read_text().strip().split("\n")
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

    records = [json.loads(l) for l in (tmp_path / "executions.jsonl").read_text().strip().split("\n")]
    stored_tags = [r["tag"] for r in records]
    assert "PROFITABLE" in stored_tags
    assert "DRY" in stored_tags


# ── New simulation gate tests ──────────────────────────────────────────────────

def test_simulation_gate_blocks_execution():
    """should_execute returns False when sim.is_executable=False."""
    opp = _make_opp(profitable=True, profit=50.0)
    sim = _make_sim(executable=False, reason="below_min_profit:2.00<10.0")
    with patch("executor.config.EXECUTE_MODE", True), \
         patch("executor.config.ARB_EXECUTOR_ADDRESS", "0xDEAD"), \
         patch("executor.config.MIN_NET_PROFIT_USD", 10.0):
        result, reason = should_execute(opp, sim)
    assert result is False
    assert "blocked_by_sim_rejection" in reason


def test_execution_uses_sim_result_amounts(tmp_path):
    """log_opportunity records sim_net_profit_usd from SimResult."""
    opp = _make_opp()
    sim = _make_sim(net_profit=42.0)
    with patch("executor.config.LOG_DIR", str(tmp_path)):
        log_opportunity(opp, "PROFITABLE", sim)

    record = json.loads((tmp_path / "executions.jsonl").read_text().strip())
    assert "sim_net_profit_usd" in record
    assert abs(record["sim_net_profit_usd"] - 42.0) < 0.01


# ── Config and isolation tests ─────────────────────────────────────────────────

def test_config_validates_on_missing_key():
    """validate() raises ValueError when BASE_RPC_URL is empty."""
    import config as cfg
    with patch.object(cfg, "BASE_RPC_URL", ""), \
         patch.object(cfg, "EXECUTE_MODE", False):
        with pytest.raises(ValueError, match="BASE_RPC_URL"):
            cfg.validate()


def test_config_validates_execute_mode_needs_key():
    """validate() raises ValueError when EXECUTE_MODE=true but PRIVATE_KEY empty."""
    import config as cfg
    with patch.object(cfg, "BASE_RPC_URL", "http://rpc"), \
         patch.object(cfg, "EXECUTE_MODE", True), \
         patch.object(cfg, "PRIVATE_KEY", ""), \
         patch.object(cfg, "ARB_EXECUTOR_ADDRESS", "0xDEAD"):
        with pytest.raises(ValueError, match="PRIVATE_KEY"):
            cfg.validate()


def test_config_all_addresses_checksummed():
    """All address constants in config must be EIP-55 checksummed."""
    from web3 import Web3
    import config as cfg

    addr_attrs = [
        "USDC_ADDRESS", "CBBTC_ADDRESS", "WETH_ADDRESS", "WEETH_ADDRESS",
        "UNISWAP_QUOTER_V2", "UNISWAP_FACTORY", "AERODROME_ROUTER",
        "AERODROME_FACTORY", "MORPHO_ADDRESS", "BALANCER_VAULT",
    ]
    for attr in addr_attrs:
        addr = getattr(cfg, attr)
        assert addr == Web3.to_checksum_address(addr), \
            f"{attr}={addr} is not checksummed"


def test_no_import_from_existing_bot():
    """New project must not import from morpho_scanner (checks import statements, not comments)."""
    import ast, os
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    violations = []
    for root, _, files in os.walk(project_dir):
        if "venv" in root:
            continue
        for fname in files:
            if not fname.endswith(".py"):
                continue
            fpath = os.path.join(root, fname)
            try:
                tree = ast.parse(open(fpath, encoding="utf-8", errors="ignore").read())
            except SyntaxError:
                continue
            for node in ast.walk(tree):
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    module = ""
                    if isinstance(node, ast.Import):
                        module = ",".join(a.name for a in node.names)
                    elif isinstance(node, ast.ImportFrom) and node.module:
                        module = node.module
                    if "morpho_scanner" in module:
                        violations.append(f"{fpath}:{node.lineno}")
    assert violations == [], f"morpho_scanner import found: {violations}"


def test_main_loop_does_not_crash_on_price_error():
    """run_cycle must not raise even if get_all_prices throws."""
    from main import run_cycle, CycleStats
    w3 = MagicMock()
    stats = CycleStats()

    with patch("main.get_all_prices", side_effect=Exception("RPC error")):
        # Must not raise
        run_cycle(w3, None, stats)

    assert stats.cycles == 0  # Nothing recorded since prices failed


# ── New: EXECUTION_READY + ExecutionResult tests ──────────────────────────────

def test_execution_ready_false_when_no_address():
    """EXECUTION_READY matches whether ARB_EXECUTOR_ADDRESS is set."""
    import config as cfg
    expected = bool(cfg.ARB_EXECUTOR_ADDRESS.strip())
    assert cfg.EXECUTION_READY == expected


def test_execute_arb_stub_when_no_contract():
    """execute_arb returns ExecutionResult(tag='STUB') when EXECUTION_READY=False."""
    from executor import execute_arb, ExecutionResult
    opp = _make_opp(profitable=True, profit=50.0)
    sim = _make_sim(executable=True)
    w3_mock = MagicMock()

    with patch("executor.config.EXECUTION_READY", False):
        result = execute_arb(w3_mock, opp, sim)

    assert isinstance(result, ExecutionResult)
    assert result.tag == "STUB"
    assert result.reason == "no_contract_address"
    assert result.estimated_profit_usd == sim.net_profit_usd
