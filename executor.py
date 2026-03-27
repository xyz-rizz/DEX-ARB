"""
Executor for DEX Arbitrage Bot.
Phase 1: scan-and-simulate — execute_arb() returns an honest stub while
ARB_EXECUTOR_ADDRESS is not set (contract not deployed).
Simulation gate: simulate_arb() must pass before any execution attempt.
Never imports from the morpho_scanner liquidation bot.
"""

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from arb_detector import ArbOpportunity, SimResult, simulate_arb
import config

logger = logging.getLogger(__name__)


# ── Execution result ──────────────────────────────────────────────────────────

@dataclass
class ExecutionResult:
    """Result returned by execute_arb()."""
    tag: str                       # STUB / DRY / SENT / ERROR
    tx_hash: str = ""
    estimated_profit_usd: float = 0.0
    actual_profit_usd: float = 0.0
    reason: str = ""               # human-readable status note
    error: str = ""


# ── Utilities ──────────────────────────────────────────────────────────────────

def _ensure_log_dir() -> Path:
    """Create log directory if it doesn't exist."""
    log_dir = Path(config.LOG_DIR)
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


# ── Execution gate ─────────────────────────────────────────────────────────────

def should_execute(opp: ArbOpportunity, sim: SimResult = None) -> tuple:
    """
    Determine whether to execute the arbitrage trade.

    Gates (all must pass):
    1. opp.is_profitable
    2. opp.estimated_profit_usdc >= MIN_NET_PROFIT_USD
    3. EXECUTE_MODE == True
    4. ARB_EXECUTOR_ADDRESS set (non-empty) — contract must be deployed
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
        return False, "execution_not_ready_no_contract"

    if sim is not None and not sim.is_executable:
        return False, f"simulation_rejected:{sim.rejection_reason}"

    return True, ""


# ── JSONL logger ───────────────────────────────────────────────────────────────

def log_opportunity(opp: ArbOpportunity, tag: str, sim: SimResult = None) -> None:
    """
    Append one opportunity record to logs/executions.jsonl.

    tag values:
        EXECUTABLE  — sim passed; execution pending or attempted
        DRY         — would execute, but DRY_RUN=true or contract not deployed
        SKIP        — sim passed but should_execute() returned False
        SENT        — tx submitted on-chain
        ERROR       — exception during execution
        PROFITABLE  — backward-compat: sim not yet run (legacy path)
        BELOW_THRESHOLD — spread too small
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


# ── Execute arb ───────────────────────────────────────────────────────────────

def execute_arb(w3_exec, opp: ArbOpportunity, sim: SimResult,
                dry_run: bool = True) -> ExecutionResult:
    """
    Execute (or dry-run) the arbitrage trade.

    While ARB_EXECUTOR_ADDRESS is not set (EXECUTION_READY=False), returns an
    honest stub explaining what is needed. No transaction is ever built or sent.

    When EXECUTION_READY=True (post contract deployment), implement real path:
      1. Build ArbParams struct from opp/sim
      2. build_transaction() via w3_exec (Alchemy)
      3. If dry_run=True: log params, return DRY
      4. If dry_run=False: sign + send + wait receipt, return SENT

    Parameters
    ----------
    w3_exec : Web3
        Execution provider (Alchemy). Used only when EXECUTION_READY=True.
    opp : ArbOpportunity
        The arbitrage candidate (sim-validated).
    sim : SimResult
        Simulation result that passed the is_executable gate.
    dry_run : bool
        True = log only, no tx sent. False = live execution.
    """
    if not config.EXECUTION_READY:
        logger.info(
            "EXEC_STUB | scan-only mode | %s | sim_profit=$%.2f | "
            "would need: deploy ArbExecutor.sol then set ARB_EXECUTOR_ADDRESS in .env",
            opp.pair,
            sim.net_profit_usd if sim else 0.0,
        )
        return ExecutionResult(
            tag="STUB",
            reason="no_contract_address",
            estimated_profit_usd=sim.net_profit_usd if sim else 0.0,
        )

    # ── Real execution path (post-deployment) ────────────────────────────────
    # Reached only when ARB_EXECUTOR_ADDRESS is non-empty.
    # TODO: implement Phase 6 on-chain execution here.
    logger.info(
        "EXEC_DRY_RUN | %s buy=%s sell=%s profit=$%.2f sim_net=$%.2f",
        opp.pair, opp.buy_venue, opp.sell_venue,
        opp.estimated_profit_usdc,
        sim.net_profit_usd if sim else 0,
    )
    log_opportunity(opp, "DRY", sim)
    return ExecutionResult(
        tag="DRY",
        reason="dry_run_mode",
        estimated_profit_usd=sim.net_profit_usd if sim else 0.0,
    )
