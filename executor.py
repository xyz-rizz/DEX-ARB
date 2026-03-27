"""
Executor for DEX Arbitrage Bot.
Phase 1: scan-only — execution stubs only. Phase 6 will add real on-chain execution.
Simulation gate added: simulate_arb() must pass before any execution.
Never imports from the morpho_scanner liquidation bot.
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from arb_detector import ArbOpportunity, SimResult, simulate_arb
import config

logger = logging.getLogger(__name__)


def _ensure_log_dir() -> Path:
    """Create log directory if it doesn't exist."""
    log_dir = Path(config.LOG_DIR)
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


def should_execute(opp: ArbOpportunity, sim: SimResult = None) -> tuple:
    """
    Determine whether to execute the arbitrage trade.

    Gates (all must pass):
    1. opp.is_profitable
    2. opp.estimated_profit_usdc >= MIN_NET_PROFIT_USD
    3. EXECUTE_MODE == True
    4. ARB_EXECUTOR_ADDRESS set (non-empty)
    5. sim.is_executable (if SimResult provided)

    Returns:
        (True, "") if all gates pass
        (False, reason_str) if any gate fails
    """
    if not opp.is_profitable:
        return False, "not profitable after fees"

    if opp.estimated_profit_usdc < config.MIN_NET_PROFIT_USD:
        return False, (
            f"profit ${opp.estimated_profit_usdc:.2f} < "
            f"min ${config.MIN_NET_PROFIT_USD:.2f}"
        )

    if not config.EXECUTE_MODE:
        return False, "EXECUTE_MODE=false"

    if not config.ARB_EXECUTOR_ADDRESS:
        return False, "ARB_EXECUTOR_ADDRESS not set — run deploy/deploy.py"

    if sim is not None and not sim.is_executable:
        return False, f"simulation_rejected:{sim.rejection_reason}"

    return True, ""


def log_opportunity(opp: ArbOpportunity, tag: str, sim: SimResult = None) -> None:
    """
    Append one opportunity record to logs/executions.jsonl.

    tag values:
        PROFITABLE      — above threshold, not yet executed
        BELOW_THRESHOLD — spread too small
        DRY             — would execute, but DRY_RUN=true
        SKIP            — should_execute() returned False
        ERROR           — exception during execution
    """
    log_dir = _ensure_log_dir()
    path = log_dir / "executions.jsonl"

    record = {
        "timestamp":             datetime.now(timezone.utc).isoformat(),
        "tag":                   tag,
        "pair":                  opp.pair,
        "buy_venue":             opp.buy_venue,
        "sell_venue":            opp.sell_venue,
        "buy_price":             round(opp.buy_price, 4),
        "sell_price":            round(opp.sell_price, 4),
        "gross_spread_pct":      round(opp.gross_spread_pct, 4),
        "net_spread_pct":        round(opp.net_spread_pct, 4),
        "tier":                  opp.tier,
        "flash_loan_usdc":       round(opp.flash_loan_usdc, 2),
        "estimated_profit_usdc": round(opp.estimated_profit_usdc, 2),
        "flash_provider":        opp.flash_provider,
        # simulation fields (populated if sim was run)
        "sim_token_amount":      round(sim.token_amount, 8) if sim else 0,
        "sim_usdc_out":          round(sim.usdc_out, 4) if sim else 0,
        "sim_gas_cost_usd":      round(sim.gas_cost_usd, 4) if sim else 0,
        "sim_net_profit_usd":    round(sim.net_profit_usd, 4) if sim else 0,
        "sim_rejection":         sim.rejection_reason if sim else "",
        # execution fields
        "tx_hash":               "",
        "actual_profit_usdc":    0,
        "error":                 "",
    }

    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record) + "\n")

    logger.debug("Logged opportunity tag=%s pair=%s profit=$%.2f",
                 tag, opp.pair, opp.estimated_profit_usdc)


def execute_arb(opp: ArbOpportunity, sim: SimResult = None) -> dict:
    """
    Phase 1 stub — execution not yet implemented.
    Logs DRY and returns a placeholder result dict.

    Phase 6 will replace this with real on-chain execution via ArbExecutor.sol.
    """
    logger.info(
        "DRY_RUN execute_arb | %s buy=%s sell=%s profit=$%.2f sim_net=$%.2f",
        opp.pair,
        opp.buy_venue,
        opp.sell_venue,
        opp.estimated_profit_usdc,
        sim.net_profit_usd if sim else 0,
    )
    log_opportunity(opp, "DRY", sim)
    return {
        "success":            False,
        "tx_hash":            "",
        "actual_profit_usdc": 0.0,
        "error":              "execution not yet implemented — deploy ArbExecutor.sol first",
    }
