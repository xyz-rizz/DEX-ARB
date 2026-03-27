"""
Executor for DEX Arbitrage Bot.
Contract deployed. execute_arb() builds and optionally sends live transactions.
Simulation gate: simulate_arb() must pass before any execution attempt.
Never imports from the morpho_scanner liquidation bot.
Server: /home/ubuntu/dexarb on ec2-44-202-120-86.compute-1.amazonaws.com
"""

import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from web3 import Web3
from arb_detector import ArbOpportunity, SimResult, simulate_arb, _estimate_eth_price
import config

# ── Contract ABI / tx-building helpers ───────────────────────────────────────

_ROOT     = Path(__file__).resolve().parent
_ABI_PATH = _ROOT / "contracts" / "ArbExecutor.abi.json"

# Per-pair execution params for tx construction.
# uniFee: Uniswap V3 fee tier (integer, e.g. 500)
# aero_tick: Aerodrome Slipstream tick spacing (integer, e.g. 1)
_PAIR_EXEC_PARAMS: dict = {
    "cbBTC/USDC":   {"uni_fee": 500,   "aero_tick": 1},
    "weETH/WETH":   {"uni_fee": 100,   "aero_tick": 1},
    "cbETH/WETH":   {"uni_fee": 500,   "aero_tick": 1},
    "wstETH/WETH":  {"uni_fee": 100,   "aero_tick": 1},
    "WETH/USDC":    {"uni_fee": 500,   "aero_tick": 200},
    "USDC/USDbC":   {"uni_fee": 100,   "aero_tick": 1},
    "DAI/USDC":     {"uni_fee": 100,   "aero_tick": 50},
    "AERO/WETH":    {"uni_fee": 3000,  "aero_tick": 200},
    "DEGEN/WETH":   {"uni_fee": 3000,  "aero_tick": 200},
    "BRETT/WETH":   {"uni_fee": 10000, "aero_tick": 200},
    "VIRTUAL/WETH": {"uni_fee": 3000,  "aero_tick": 200},
    "TOSHI/WETH":   {"uni_fee": 10000, "aero_tick": 200},
    "cbXRP/USDC":   {"uni_fee": 500,   "aero_tick": 200},
    "MOG/WETH":     {"uni_fee": 10000, "aero_tick": 200},
    "HIGHER/WETH":  {"uni_fee": 3000,  "aero_tick": 200},
}
_DEFAULT_EXEC_PARAMS = {"uni_fee": 500, "aero_tick": 50}


def _load_abi() -> list:
    """Load compiled ArbExecutor ABI. Created by deploy/deploy.py."""
    if not _ABI_PATH.exists():
        raise FileNotFoundError(
            f"ABI not found at {_ABI_PATH} — run: python deploy/deploy.py"
        )
    return json.loads(_ABI_PATH.read_text())


def _build_arb_params(
    opp: ArbOpportunity,
    sim: SimResult,
    eth_price: float,
) -> tuple:
    """
    Construct an ArbParams tuple for executeArb().
    Field order must match ArbExecutor.sol struct definition exactly.

    Struct fields (in order):
      tokenBorrow, tokenIntermediate, uniFee, aeroTickSpacing,
      flashLoanAmount, minIntermediate, minRepayToken,
      minProfit, deadline, buyOnUniswap
    """
    pair_cfg = next(
        (p for p in config.PAIR_CONFIG if p["name"] == opp.pair), None
    )
    if pair_cfg is None:
        raise ValueError(f"pair_config not found for {opp.pair}")

    token_borrow = pair_cfg["token_out"]
    token_inter  = pair_cfg["token_in"]
    dec_out = pair_cfg["dec_out"]
    dec_in  = pair_cfg["dec_in"]

    is_weth_borrow = token_borrow.lower() == config.WETH_ADDRESS.lower()

    # Flash loan amount in raw borrow-token units
    if is_weth_borrow:
        borrow_human = opp.flash_loan_usdc / max(eth_price, 1.0)
    else:
        borrow_human = opp.flash_loan_usdc
    flash_loan_raw = int(borrow_human * (10 ** dec_out))

    # Min intermediate received from buy leg (2% slippage buffer)
    min_inter_raw = int(sim.token_amount * 0.98 * (10 ** dec_in))

    # Min repay token from sell leg (2% slippage buffer)
    # sim.usdc_out is in USD terms; convert back to borrow-token units
    if is_weth_borrow:
        repay_human = sim.usdc_out / max(eth_price, 1.0)
    else:
        repay_human = sim.usdc_out
    min_repay_raw = int(repay_human * 0.98 * (10 ** dec_out))

    # Min profit in raw borrow-token units
    if is_weth_borrow:
        min_profit_raw = int(
            (config.MIN_NET_PROFIT_USD / max(eth_price, 1.0)) * (10 ** dec_out)
        )
    else:
        min_profit_raw = int(config.MIN_NET_PROFIT_USD * (10 ** dec_out))

    # Buy-on-Uniswap flag
    buy_on_uni = any(
        v in opp.buy_venue
        for v in ("Uniswap", "PancakeSwap", "BaseSwap")
    )

    # Fee / tick-spacing lookup
    ep = _PAIR_EXEC_PARAMS.get(opp.pair, _DEFAULT_EXEC_PARAMS)

    return (
        Web3.to_checksum_address(token_borrow),   # tokenBorrow
        Web3.to_checksum_address(token_inter),     # tokenIntermediate
        ep["uni_fee"],                             # uniFee (uint24)
        ep["aero_tick"],                           # aeroTickSpacing (int24)
        flash_loan_raw,                            # flashLoanAmount
        min_inter_raw,                             # minIntermediate
        min_repay_raw,                             # minRepayToken
        min_profit_raw,                            # minProfit
        int(time.time()) + 60,                     # deadline
        buy_on_uni,                                # buyOnUniswap
    )

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
    2a. Pre-sim early filter: if sim is None, opp.estimated_profit_usdc >= MIN_NET_PROFIT_USD
    2b. Post-sim profit check: if sim is provided, sim.net_profit_usd >= MIN_NET_PROFIT_USD
    3. EXECUTE_MODE == True
    4. ARB_EXECUTOR_ADDRESS set (non-empty) — contract must be deployed
    5. sim.is_executable (if SimResult provided)

    Returns:
        (True, "") if all gates pass
        (False, reason_str) if any gate fails
    """
    # Gate 1: must be profitable after fees
    if not opp.is_profitable:
        return False, "blocked_by_not_profitable"

    # Gate 2a: pre-sim early filter (fast reject before running simulation)
    if sim is None and opp.estimated_profit_usdc < config.MIN_NET_PROFIT_USD:
        return False, "blocked_by_pre_sim_estimate:${:.2f}<${:.2f}".format(
            opp.estimated_profit_usdc, config.MIN_NET_PROFIT_USD
        )

    # Gate 2b: post-sim profit check (overrides pre-sim estimate when sim is available)
    if sim is not None and sim.net_profit_usd < config.MIN_NET_PROFIT_USD:
        return False, "blocked_by_sim_profit:${:.2f}<${:.2f}".format(
            sim.net_profit_usd, config.MIN_NET_PROFIT_USD
        )

    # Gate 3: execute mode must be enabled
    if not config.EXECUTE_MODE:
        return False, "blocked_by_execute_mode"

    # Gate 4: contract must be deployed
    if not config.ARB_EXECUTOR_ADDRESS:
        return False, "blocked_by_no_contract"

    # Gate 5: simulation must approve execution
    if sim is not None and not sim.is_executable:
        return False, "blocked_by_sim_rejection:{}".format(sim.rejection_reason)

    return True, ""


# ── JSONL logger ───────────────────────────────────────────────────────────────

def log_opportunity(opp: ArbOpportunity, tag: str, sim: SimResult = None) -> None:
    """
    Append one opportunity record to logs/executions.jsonl.

    tag values:
        DRY         — tx built but not sent (DRY_RUN=true)
        SKIP        — sim passed but should_execute() returned False
        SENT        — tx submitted on-chain
        STUB        — contract not deployed, scan-only mode
        ERROR       — exception during execution
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
        "estimated_profit_usdc":   round(opp.estimated_profit_usdc, 2),
        "flash_provider":          opp.flash_provider,
        # simulation fields (populated if sim was run)
        "sim_token_amount":        round(sim.token_amount, 8) if sim else 0,
        "sim_usdc_out":            round(sim.usdc_out, 4) if sim else 0,
        "sim_gas_cost_usd":        round(sim.gas_cost_usd, 4) if sim else 0,
        "sim_net_profit_usd":      round(sim.net_profit_usd, 4) if sim else 0,
        "profit_after_slippage_usd": round(sim.net_profit_usd, 4) if sim else 0,
        "sim_rejection":           sim.rejection_reason if sim else "",
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
    try:
        from eth_account import Account

        abi    = _load_abi()
        wallet = Account.from_key(config.PRIVATE_KEY).address

        if w3_exec is None:
            raise RuntimeError("w3_exec is None — ALCHEMY_EXEC_URL not set")

        eth_price = _estimate_eth_price(w3_exec)

        contract = w3_exec.eth.contract(
            address=Web3.to_checksum_address(config.ARB_EXECUTOR_ADDRESS),
            abi=abi,
        )

        arb_params = _build_arb_params(opp, sim, eth_price)
        provider_int = 1 if (sim and sim.flash_provider == "Balancer") else 0

        gas_price = w3_exec.eth.gas_price
        nonce     = w3_exec.eth.get_transaction_count(wallet)

        tx = contract.functions.executeArb(
            arb_params, provider_int
        ).build_transaction({
            "from":     wallet,
            "gas":      600_000,
            "gasPrice": gas_price * 2,
            "nonce":    nonce,
            "chainId":  config.CHAIN_ID,
        })

        logger.info(
            "DRY_RUN | %s | to=%s | gas=%d | data=%s... | est_profit=$%.2f",
            opp.pair,
            tx["to"],
            tx["gas"],
            tx["data"][:66],
            sim.net_profit_usd if sim else 0.0,
        )

        if dry_run or config.DRY_RUN:
            return ExecutionResult(
                tag="DRY",
                reason=f"to={tx['to']} gas={tx['gas']}",
                estimated_profit_usd=sim.net_profit_usd if sim else 0.0,
            )

        # ── Live execution (only when both dry_run=False AND DRY_RUN=false) ──
        signed  = w3_exec.eth.account.sign_transaction(tx, config.PRIVATE_KEY)
        tx_hash = w3_exec.eth.send_raw_transaction(signed.rawTransaction)
        logger.info("SENT | %s | tx_hash=%s", opp.pair, tx_hash.hex())

        receipt = w3_exec.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
        if receipt.status != 1:
            raise RuntimeError(f"tx reverted: {tx_hash.hex()}")

        # Read profit from ArbExecuted event
        actual_profit = 0.0
        try:
            events = contract.events.ArbExecuted().process_receipt(receipt)
            if events:
                raw_profit = events[0]["args"]["profit"]
                dec_out = next(
                    (p["dec_out"] for p in config.PAIR_CONFIG if p["name"] == opp.pair),
                    6,
                )
                actual_profit = raw_profit / (10 ** dec_out)
                if pair_cfg := next(
                    (p for p in config.PAIR_CONFIG if p["name"] == opp.pair), None
                ):
                    is_weth_borrow = pair_cfg["token_out"].lower() == config.WETH_ADDRESS.lower()
                    if is_weth_borrow:
                        actual_profit *= eth_price
        except Exception:
            pass

        return ExecutionResult(
            tag="SENT",
            tx_hash=tx_hash.hex(),
            actual_profit_usd=actual_profit,
            estimated_profit_usd=sim.net_profit_usd if sim else 0.0,
        )

    except Exception as exc:
        logger.error("execute_arb failed for %s: %s", opp.pair, exc, exc_info=True)
        return ExecutionResult(tag="ERROR", error=str(exc))
