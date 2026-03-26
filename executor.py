"""
Executor for DEX Arbitrage Bot — Phase 1: scan-only.
Execution functions are stubs that log DRY/SKIP only.
No actual transaction building in this phase.
Never imports from the morpho_scanner liquidation bot.
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from arb_detector import ArbOpportunity
import config

logger = logging.getLogger(__name__)


def _ensure_log_dir() -> Path:
    """Create log directory if it doesn't exist."""
    log_dir = Path(config.LOG_DIR)
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


def should_execute(opp: ArbOpportunity) -> tuple:
    """
    Determine whether to execute the arbitrage trade.

    Gates (all must pass):
    1. opp.is_profitable
    2. opp.estimated_profit_usdc >= MIN_NET_PROFIT_USD
    3. EXECUTE_MODE == True
    4. ARB_EXECUTOR_ADDRESS is set (non-empty)

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

    return True, ""


def log_opportunity(opp: ArbOpportunity, tag: str) -> None:
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
        "timestamp":           datetime.now(timezone.utc).isoformat(),
        "tag":                 tag,
        "pair":                opp.pair,
        "buy_venue":           opp.buy_venue,
        "sell_venue":          opp.sell_venue,
        "buy_price":           round(opp.buy_price, 4),
        "sell_price":          round(opp.sell_price, 4),
        "gross_spread_pct":    round(opp.gross_spread_pct, 4),
        "net_spread_pct":      round(opp.net_spread_pct, 4),
        "flash_loan_usdc":     round(opp.flash_loan_usdc, 2),
        "estimated_profit_usdc": round(opp.estimated_profit_usdc, 2),
        "tx_hash":             "",
        "actual_profit_usdc":  0,
        "error":               "",
    }

    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record) + "\n")

    logger.debug("Logged opportunity tag=%s pair=%s profit=$%.2f",
                 tag, opp.pair, opp.estimated_profit_usdc)


def execute_arb(opp: ArbOpportunity) -> dict:
    """
    Phase 1 stub — execution not yet implemented.
    Logs DRY and returns a placeholder result dict.

    Phase 6 will replace this with real on-chain execution via ArbExecutor.sol.
    """
    logger.info(
        "DRY_RUN execute_arb | %s buy=%s sell=%s profit=$%.2f",
        opp.pair, opp.buy_venue, opp.sell_venue, opp.estimated_profit_usdc,
    )
    log_opportunity(opp, "DRY")
    return {
        "success": False,
        "tx_hash": "",
        "actual_profit_usdc": 0.0,
        "error": "execution not yet implemented — deploy ArbExecutor.sol first",
    }
