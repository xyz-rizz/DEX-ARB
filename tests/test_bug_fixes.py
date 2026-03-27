"""
Tests for 4 confirmed bugs fixed:
  Bug 1: PROFITABLE tag never written to executions.jsonl
  Bug 2: DRY/SENT/ERROR written exactly once (no duplicate from inside execute_arb)
  Bug 3: DEPTH_REJECTED opps never reach simulate_arb
  Bug 4: RPC errors appear as WARNING in stdout; rpc_errors=N in LATENCY line
"""

import time
import logging
import pytest
from unittest.mock import patch, MagicMock

from arb_detector import ArbOpportunity, SimResult
from executor import ExecutionResult


# ── Shared helpers ─────────────────────────────────────────────────────────────

def _opp(pair="cbBTC/USDC", profitable=True, profit=31.0, tier="MARGINAL"):
    return ArbOpportunity(
        pair=pair,
        buy_venue="Uniswap V3",
        sell_venue="Aerodrome Slipstream",
        buy_price=68000.0,
        sell_price=68200.0,
        gross_spread_pct=0.294,
        total_fee_pct=0.06,
        net_spread_pct=0.234,
        flash_loan_usdc=17000.0,
        estimated_profit_usdc=profit,
        is_profitable=profitable,
        timestamp=time.time(),
        tier=tier,
    )


def _sim(executable=True, reason="", net_profit=15.0):
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


# ── Bug 1: PROFITABLE tag never written ────────────────────────────────────────

def test_profitable_tag_never_written_to_jsonl(tmp_path):
    """run_cycle must never call log_opportunity with tag='PROFITABLE'."""
    from main import run_cycle, CycleStats

    opp = _opp()
    sim = _sim(executable=True)
    tags_logged = []

    with patch("main.get_all_prices", return_value={"cbBTC/USDC": []}), \
         patch("main.detect_all_opportunities", return_value=[opp]), \
         patch("main.simulate_arb", return_value=sim), \
         patch("main.log_opportunity",
               side_effect=lambda o, tag, sim=None: tags_logged.append(tag)), \
         patch("main.should_execute", return_value=(False, "EXECUTE_MODE=false")), \
         patch("main.config.LOG_DIR", str(tmp_path)):
        run_cycle(MagicMock(), None, CycleStats())

    assert "PROFITABLE" not in tags_logged, (
        f"PROFITABLE tag must never be written to executions.jsonl; got: {tags_logged}"
    )


# ── Bug 2: DRY written exactly once ────────────────────────────────────────────

def test_dry_written_exactly_once(tmp_path):
    """When execute_arb returns tag='DRY', log_opportunity must be called exactly once."""
    from main import run_cycle, CycleStats

    opp = _opp()
    sim = _sim(executable=True)
    dry_result = ExecutionResult(tag="DRY", estimated_profit_usd=15.0)
    tags_logged = []

    with patch("main.get_all_prices", return_value={"cbBTC/USDC": []}), \
         patch("main.detect_all_opportunities", return_value=[opp]), \
         patch("main.simulate_arb", return_value=sim), \
         patch("main.execute_arb", return_value=dry_result), \
         patch("main.should_execute", return_value=(True, "")), \
         patch("main.log_opportunity",
               side_effect=lambda o, tag, s=None: tags_logged.append(tag)), \
         patch("main.config.LOG_DIR", str(tmp_path)):
        run_cycle(MagicMock(), MagicMock(), CycleStats())

    dry_count = tags_logged.count("DRY")
    assert dry_count == 1, (
        f"Expected exactly 1 DRY log; got {dry_count}. All tags: {tags_logged}"
    )


# ── Bug 3: DEPTH_REJECTED never reaches simulate_arb ──────────────────────────

def test_depth_zero_never_reaches_simulate(tmp_path):
    """
    When find_max_executable_size returns 0, simulate_arb must not be called
    and no PROFITABLE or SKIP tag must appear in the log.
    """
    from main import run_cycle, CycleStats
    from price_scanner import PriceQuote

    # A ~3% spread for AERO/WETH (below the 5% sanity cap) — would normally be profitable.
    prices = {
        "AERO/WETH": [
            PriceQuote(venue="Uniswap V3",     pair="AERO/WETH",
                       price=0.000160, fee_pct=0.0030, block=0, timestamp=0.0),
            PriceQuote(venue="PancakeSwap V3", pair="AERO/WETH",
                       price=0.000165, fee_pct=0.0025, block=0, timestamp=0.0),
        ]
    }
    tags_logged = []

    with patch("main.get_all_prices", return_value=prices), \
         patch("arb_detector.find_max_executable_size", return_value=0.0), \
         patch("main.simulate_arb") as mock_sim, \
         patch("main.log_opportunity",
               side_effect=lambda o, tag, s=None: tags_logged.append(tag)), \
         patch("main.config.LOG_DIR", str(tmp_path)):
        run_cycle(MagicMock(), None, CycleStats())

    mock_sim.assert_not_called()
    assert "PROFITABLE" not in tags_logged
    assert "SKIP" not in tags_logged


# ── Bug 4: RPC errors visible in stdout ────────────────────────────────────────

def test_rpc_error_logged_to_stdout(caplog, capsys):
    """
    Price-fetch RPC errors must be logged at WARNING level and rpc_errors=1
    must appear in the LATENCY line printed to stdout.
    """
    from main import run_cycle, CycleStats

    with caplog.at_level(logging.WARNING, logger="main"), \
         patch("main.get_all_prices",
               side_effect=Exception("{'code': -32000, 'message': 'internal error'}")):
        run_cycle(MagicMock(), None, CycleStats())

    # WARNING must be logged (not just ERROR going to errors.log)
    warning_records = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert any("price_fetch_failed" in r.message for r in warning_records), (
        f"Expected WARNING with 'price_fetch_failed'; "
        f"got records: {[r.message for r in caplog.records]}"
    )

    # LATENCY line must include rpc_errors=1
    captured = capsys.readouterr()
    assert "rpc_errors=1" in captured.out, (
        f"Expected 'rpc_errors=1' in LATENCY output; stdout:\n{captured.out}"
    )
