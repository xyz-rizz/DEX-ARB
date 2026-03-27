"""
Tests for the dry_run pass-through fix:
  - main.py must pass dry_run=config.DRY_RUN explicitly to execute_arb()
  - execute_arb must honour dry_run=False (not force DRY via default leakage)
  - execute_arb with config.DRY_RUN=True must return DRY regardless of the
    dry_run argument value
  - only one JSONL record written per execution event
"""

import time
import pytest
from unittest.mock import patch, MagicMock, call

from arb_detector import ArbOpportunity, SimResult
from executor import ExecutionResult


# ── Shared helpers ─────────────────────────────────────────────────────────────

def _opp(pair="WETH/USDC", profit=4.0, tier="MARGINAL"):
    return ArbOpportunity(
        pair=pair,
        buy_venue="Aerodrome Slipstream",
        sell_venue="PancakeSwap V3",
        buy_price=1986.0,
        sell_price=1987.0,
        gross_spread_pct=0.0504,
        total_fee_pct=0.02,
        net_spread_pct=0.0304,
        flash_loan_usdc=5000.0,
        estimated_profit_usdc=profit,
        is_profitable=True,
        timestamp=time.time(),
        tier=tier,
    )


def _sim(executable=True, net_profit=4.0):
    return SimResult(
        buy_dex="Aerodrome Slipstream",
        sell_dex="PancakeSwap V3",
        token_amount=2.51,
        usdc_in=5000.0,
        usdc_out=5004.1,
        gross_profit_usd=4.1,
        gas_cost_usd=0.1,
        net_profit_usd=net_profit,
        flash_provider="Morpho",
        is_executable=executable,
        rejection_reason="",
    )


# ── Test 1: main passes dry_run=config.DRY_RUN (DRY_RUN=True case) ─────────────

def test_main_passes_dry_run_true_to_execute_arb(tmp_path):
    """When config.DRY_RUN=True, run_cycle must call execute_arb(dry_run=True)."""
    from main import run_cycle, CycleStats

    opp = _opp()
    sim = _sim()
    dry_run_kwargs = []

    def capture_execute(w3, o, s, dry_run=True):
        dry_run_kwargs.append(dry_run)
        return ExecutionResult(tag="DRY", estimated_profit_usd=s.net_profit_usd)

    with patch("main.get_all_prices", return_value={"WETH/USDC": []}), \
         patch("main.detect_all_opportunities", return_value=[opp]), \
         patch("main.simulate_arb", return_value=sim), \
         patch("main.execute_arb", side_effect=capture_execute), \
         patch("main.should_execute", return_value=(True, "")), \
         patch("main.log_opportunity", side_effect=lambda o, tag, s=None: None), \
         patch("main.config.DRY_RUN", True), \
         patch("main.config.LOG_DIR", str(tmp_path)):
        run_cycle(MagicMock(), MagicMock(), CycleStats())

    assert len(dry_run_kwargs) == 1, f"execute_arb not called once; calls={dry_run_kwargs}"
    assert dry_run_kwargs[0] is True, (
        f"expected dry_run=True (matching config.DRY_RUN=True), got {dry_run_kwargs[0]}"
    )


# ── Test 2: main passes dry_run=config.DRY_RUN (DRY_RUN=False case) ────────────

def test_main_passes_dry_run_false_to_execute_arb(tmp_path):
    """When config.DRY_RUN=False, run_cycle must call execute_arb(dry_run=False).
    This is the fix for the default-leakage bug."""
    from main import run_cycle, CycleStats

    opp = _opp()
    sim = _sim()
    dry_run_kwargs = []

    def capture_execute(w3, o, s, dry_run=True):
        dry_run_kwargs.append(dry_run)
        return ExecutionResult(tag="DRY", estimated_profit_usd=s.net_profit_usd)

    with patch("main.get_all_prices", return_value={"WETH/USDC": []}), \
         patch("main.detect_all_opportunities", return_value=[opp]), \
         patch("main.simulate_arb", return_value=sim), \
         patch("main.execute_arb", side_effect=capture_execute), \
         patch("main.should_execute", return_value=(True, "")), \
         patch("main.log_opportunity", side_effect=lambda o, tag, s=None: None), \
         patch("main.config.DRY_RUN", False), \
         patch("main.config.LOG_DIR", str(tmp_path)):
        run_cycle(MagicMock(), MagicMock(), CycleStats())

    assert len(dry_run_kwargs) == 1, f"execute_arb not called once; calls={dry_run_kwargs}"
    assert dry_run_kwargs[0] is False, (
        f"expected dry_run=False (matching config.DRY_RUN=False), got {dry_run_kwargs[0]}. "
        f"This is the default-leakage bug — main.py must pass dry_run=config.DRY_RUN explicitly."
    )


# ── Test 3: execute_arb returns DRY when config.DRY_RUN=True ──────────────────

def test_execute_arb_returns_dry_when_config_dry_run_true():
    """Even if dry_run=False is passed, config.DRY_RUN=True forces DRY return."""
    from executor import execute_arb

    opp = _opp()
    sim = _sim()
    mock_w3 = MagicMock()
    mock_w3.eth.gas_price = 1_000_000_000
    mock_w3.eth.get_transaction_count.return_value = 0

    mock_contract = MagicMock()
    mock_contract.functions.executeArb.return_value.build_transaction.return_value = {
        "to": "0x" + "c" * 40,
        "gas": 600_000,
        "data": "0x" + "a" * 66,
    }
    mock_w3.eth.contract.return_value = mock_contract

    with patch("executor.config.EXECUTION_READY", True), \
         patch("executor.config.DRY_RUN", True), \
         patch("executor.config.PRIVATE_KEY", "0x" + "a" * 64), \
         patch("executor.config.PAIR_CONFIG", [
             {"name": "WETH/USDC", "token_in": "0x" + "b"*40,
              "token_out": "0x" + "c"*40, "dec_in": 18, "dec_out": 6,
              "unit_size": 1.0, "min_liquidity_usd": 200_000}
         ]), \
         patch("executor._load_abi", return_value=[]), \
         patch("executor._estimate_eth_price", return_value=2000.0), \
         patch("eth_account.Account.from_key") as mock_acct:
        mock_acct.return_value.address = "0x" + "d" * 40
        result = execute_arb(mock_w3, opp, sim, dry_run=False)

    assert result.tag == "DRY", (
        f"Expected DRY (config.DRY_RUN=True overrides dry_run=False), got {result.tag}"
    )


# ── Test 4: execute_arb reaches live path when both False ─────────────────────

def test_execute_arb_passes_dry_gate_when_both_false():
    """execute_arb(dry_run=False) with config.DRY_RUN=False must NOT return DRY.
    We simulate a failed send → expect ERROR, proving the dry gate was cleared."""
    from executor import execute_arb

    opp = _opp()
    sim = _sim()
    mock_w3 = MagicMock()
    mock_w3.eth.gas_price = 1_000_000_000
    mock_w3.eth.get_transaction_count.return_value = 0

    mock_contract = MagicMock()
    mock_contract.functions.executeArb.return_value.build_transaction.return_value = {
        "to": "0x" + "c" * 40,
        "gas": 600_000,
        "data": "0x" + "a" * 66,
    }
    mock_w3.eth.contract.return_value = mock_contract
    mock_w3.eth.account.sign_transaction.return_value = MagicMock(
        rawTransaction=b"\x01\x02\x03"
    )
    # Simulate send failure — proves we reached the live path (past dry gate)
    mock_w3.eth.send_raw_transaction.side_effect = RuntimeError("simulated_rpc_failure")

    with patch("executor.config.EXECUTION_READY", True), \
         patch("executor.config.DRY_RUN", False), \
         patch("executor.config.PRIVATE_KEY", "0x" + "a" * 64), \
         patch("executor.config.PAIR_CONFIG", [
             {"name": "WETH/USDC", "token_in": "0x" + "b"*40,
              "token_out": "0x" + "c"*40, "dec_in": 18, "dec_out": 6,
              "unit_size": 1.0, "min_liquidity_usd": 200_000}
         ]), \
         patch("executor._load_abi", return_value=[]), \
         patch("executor._estimate_eth_price", return_value=2000.0), \
         patch("eth_account.Account.from_key") as mock_acct:
        mock_acct.return_value.address = "0x" + "d" * 40
        result = execute_arb(mock_w3, opp, sim, dry_run=False)

    assert result.tag != "DRY", (
        "execute_arb(dry_run=False, config.DRY_RUN=False) must not return DRY — "
        "dry gate should be cleared"
    )
    assert result.tag == "ERROR", (
        f"Expected ERROR from failed send (proving live path reached), got {result.tag}"
    )


# ── Test 5: exactly one JSONL record per execution event ──────────────────────

def test_single_jsonl_record_per_execution_event(tmp_path):
    """A single execution event must produce exactly one JSONL record.
    No duplicates regardless of DRY_RUN value."""
    from main import run_cycle, CycleStats

    opp = _opp()
    sim = _sim()
    log_calls = []

    def capture_log(o, tag, s=None):
        log_calls.append(tag)

    with patch("main.get_all_prices", return_value={"WETH/USDC": []}), \
         patch("main.detect_all_opportunities", return_value=[opp]), \
         patch("main.simulate_arb", return_value=sim), \
         patch("main.execute_arb",
               return_value=ExecutionResult(tag="DRY", estimated_profit_usd=4.0)), \
         patch("main.should_execute", return_value=(True, "")), \
         patch("main.log_opportunity", side_effect=capture_log), \
         patch("main.config.LOG_DIR", str(tmp_path)):
        run_cycle(MagicMock(), MagicMock(), CycleStats())

    assert log_calls.count("DRY") == 1, (
        f"Expected exactly 1 DRY log entry; got {log_calls.count('DRY')}. "
        f"All tags: {log_calls}"
    )
    assert len(log_calls) == 1, (
        f"Expected exactly 1 total log call; got {len(log_calls)}: {log_calls}"
    )
