"""
DEX Arbitrage Bot — main scan loop.
Synchronous polling. Connects to Base mainnet via CDP RPC for reads,
Alchemy for execution (when enabled). Never uses Alchemy for price reads.
Never imports from the morpho_scanner liquidation bot.
"""

import logging
import sys
import time
from collections import defaultdict
from pathlib import Path
from datetime import datetime, timezone

from web3 import Web3

import config
from price_scanner import get_all_prices
from arb_detector import (
    ArbOpportunity, SimResult,
    detect_all_opportunities, simulate_arb,
)
from executor import should_execute, log_opportunity, execute_arb

# ── Logging setup ─────────────────────────────────────────────────────────────

def _setup_logging() -> None:
    sys.stdout.reconfigure(line_buffering=True)

    log_dir = Path(config.LOG_DIR)
    log_dir.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    if root.handlers:
        return

    root.setLevel(logging.DEBUG)
    fmt = logging.Formatter(
        "%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    stdout_handler.setFormatter(fmt)
    root.addHandler(stdout_handler)

    error_log = log_dir / "errors.log"
    file_handler = logging.FileHandler(error_log, encoding="utf-8")
    file_handler.setLevel(logging.ERROR)
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)


logger = logging.getLogger("main")

# ── Banner ────────────────────────────────────────────────────────────────────

def _print_banner() -> None:
    mode = "EXECUTE" if config.EXECUTE_MODE else "SCAN_ONLY"
    pair_names = [p["name"] for p in config.PAIR_CONFIG]
    print(
        f"\n{'='*70}\n"
        f"  ARB_BOT | pairs={','.join(pair_names[:4])}...({len(pair_names)} total)\n"
        f"  mode={mode} | dexes={len(config.DEX_CONFIG)} | min_spread={config.MIN_SPREAD_PCT:.3f}%\n"
        f"  min_profit=${config.MIN_NET_PROFIT_USD:.2f} | interval={config.SCAN_INTERVAL_SECONDS}s\n"
        f"  wallet={config.WALLET_ADDRESS}\n"
        f"  contract={'(not deployed)' if not config.ARB_EXECUTOR_ADDRESS else config.ARB_EXECUTOR_ADDRESS}\n"
        f"{'='*70}\n"
    )


# ── Tier formatting ───────────────────────────────────────────────────────────

_TIER_EMOJI = {
    "PRIME":    "🔴 PRIME",
    "GOOD":     "🟢 GOOD",
    "MARGINAL": "🟡 MARGINAL",
    "BELOW":    "⚪ BELOW",
    "NO_ARB":   "❌ NO_ARB",
}


# ── Per-cycle scan ────────────────────────────────────────────────────────────

def _log_scan_line(opp: ArbOpportunity) -> None:
    """Print ARB_SCAN line for one opportunity. Always shows real profit."""
    tier_str = _TIER_EMOJI.get(opp.tier, opp.tier)
    # Suppress weETH noise below 0.05% at DEBUG level only
    if "weETH" in opp.pair and opp.net_spread_pct < 0.05:
        logger.debug(
            "ARB_SCAN | %s | spread=%.4f%% net=%.4f%% | %s (suppressed)",
            opp.pair, opp.gross_spread_pct, opp.net_spread_pct, tier_str,
        )
        return

    flash_k = f"${opp.flash_loan_usdc/1000:.0f}k" if opp.flash_loan_usdc >= 1000 else f"${opp.flash_loan_usdc:.0f}"
    print(
        f"ARB_SCAN | {opp.pair} | "
        f"{opp.buy_venue[:6]}={opp.buy_price:.4f} "
        f"{opp.sell_venue[:6]}={opp.sell_price:.4f} | "
        f"spread={opp.gross_spread_pct:.4f}% net={opp.net_spread_pct:.4f}% | "
        f"flash={flash_k} | "
        f"profit=${opp.estimated_profit_usdc:.2f} | {tier_str}"
    )


def _log_best(opp: ArbOpportunity, sim: SimResult) -> None:
    """Print ARB_BEST line for the top simulation result."""
    status = "EXECUTING" if sim.is_executable else f"SKIPPED ({sim.rejection_reason})"
    print(
        f"ARB_BEST | pair={opp.pair} | "
        f"buy={opp.buy_venue} sell={opp.sell_venue} | "
        f"sim_profit=${sim.gross_profit_usd:.2f} | "
        f"gas=${sim.gas_cost_usd:.2f} | "
        f"net=${sim.net_profit_usd:.2f} | {status}"
    )


# ── Per-cycle statistics ──────────────────────────────────────────────────────

class CycleStats:
    """Accumulate per-cycle metrics for periodic reporting."""

    def __init__(self):
        self.cycles = 0
        self.pairs_with_quotes = 0
        self.opportunities = 0
        self.simulated = 0
        self.executable = 0
        self.rejected_gas = 0
        self.rejected_slippage = 0
        self.rejected_liquidity = 0
        self.best_net_profit = 0.0
        self.best_pair = ""
        self.net_profits: list = []
        self.dex_combo_counts: dict = defaultdict(int)
        # Per-pair stats
        self.pair_hits: dict = defaultdict(int)
        self.pair_spreads: dict = defaultdict(list)
        self.pair_profits: dict = defaultdict(list)
        self.pair_exec: dict = defaultdict(int)

    def record_cycle(self, all_opps: list, top_sim: SimResult = None):
        self.cycles += 1
        self.pairs_with_quotes += len(all_opps)

        for opp in all_opps:
            if opp.is_profitable:
                self.opportunities += 1
                self.pair_hits[opp.pair] += 1
                self.pair_spreads[opp.pair].append(opp.net_spread_pct)
                self.pair_profits[opp.pair].append(opp.estimated_profit_usdc)

        if top_sim:
            self.simulated += 1
            if top_sim.is_executable:
                self.executable += 1
                self.pair_exec[top_sim.buy_dex] += 1
            elif "gas" in top_sim.rejection_reason:
                self.rejected_gas += 1
            elif "slippage" in top_sim.rejection_reason:
                self.rejected_slippage += 1
            elif "FLASH" in top_sim.rejection_reason:
                self.rejected_liquidity += 1

            if top_sim.net_profit_usd > self.best_net_profit:
                self.best_net_profit = top_sim.net_profit_usd
                # find pair from top_sim.buy_dex (best we can without passing opp)

        # Track best net profit from estimated_profit
        for opp in all_opps:
            if opp.is_profitable:
                self.net_profits.append(opp.estimated_profit_usdc)
                if opp.estimated_profit_usdc > self.best_net_profit:
                    self.best_net_profit = opp.estimated_profit_usdc
                    self.best_pair = opp.pair
                combo = f"{opp.buy_venue}→{opp.sell_venue}"
                self.dex_combo_counts[combo] += 1

    def emit_stats(self):
        avg = sum(self.net_profits) / len(self.net_profits) if self.net_profits else 0
        top_combo = max(self.dex_combo_counts, key=self.dex_combo_counts.get) if self.dex_combo_counts else "N/A"
        print(
            f"ARB_STATS | cycles={self.cycles} | pairs_scanned={self.pairs_with_quotes} | "
            f"opportunities={self.opportunities} | simulated={self.simulated} | "
            f"executable={self.executable} | "
            f"rejected_gas={self.rejected_gas} | rejected_slippage={self.rejected_slippage} | "
            f"rejected_liquidity={self.rejected_liquidity} | "
            f"best_pair={self.best_pair} | best_net=${self.best_net_profit:.2f} | "
            f"avg_net=${avg:.2f} | top_dex_combo={top_combo}"
        )
        self.net_profits.clear()

    def emit_pair_stats(self):
        for pair, hits in sorted(self.pair_hits.items(), key=lambda x: -x[1]):
            spreads = self.pair_spreads.get(pair, [])
            profits = self.pair_profits.get(pair, [])
            exec_rate = (self.pair_exec.get(pair, 0) / hits * 100) if hits > 0 else 0
            avg_spread = sum(spreads) / len(spreads) if spreads else 0
            avg_profit = sum(profits) / len(profits) if profits else 0
            print(
                f"ARB_PAIR_STATS | pair={pair} | hits={hits} | "
                f"avg_spread={avg_spread:.4f}% | avg_profit=${avg_profit:.2f} | "
                f"executable_rate={exec_rate:.0f}%"
            )


# ── Main cycle ────────────────────────────────────────────────────────────────

def run_cycle(w3_read: Web3, stats: CycleStats = None) -> None:
    """Execute one scan cycle — fetch prices, detect opportunities, log and optionally execute."""
    try:
        prices = get_all_prices(w3_read)
    except Exception as exc:
        logger.error("Price fetch failed: %s", exc)
        return

    if not prices:
        logger.debug("No prices returned this cycle")
        return

    # Detect all opportunities for all pairs
    try:
        all_opps = detect_all_opportunities(
            prices=prices,
            min_spread_pct=config.MIN_SPREAD_PCT,
            max_flash_usdc=config.MAX_FLASH_LOAN_USDC,
        )
    except Exception as exc:
        logger.error("detect_all_opportunities failed: %s", exc)
        return

    # Log scan line for every opportunity (profitable or not)
    for opp in all_opps:
        try:
            _log_scan_line(opp)
        except Exception as exc:
            logger.error("scan line for %s failed: %s", opp.pair, exc)

    # Log profitable ones to JSONL
    profitable = [o for o in all_opps if o.is_profitable]
    for opp in profitable:
        try:
            log_opportunity(opp, "PROFITABLE")
        except Exception as exc:
            logger.error("log_opportunity failed: %s", exc)

    # Pick top 3 profitable for simulation
    top3 = profitable[:3]
    top_sim: SimResult = None
    top_opp: ArbOpportunity = None

    for opp in top3:
        try:
            sim = simulate_arb(w3_read, opp)
            _log_best(opp, sim)

            if top_sim is None or sim.net_profit_usd > top_sim.net_profit_usd:
                top_sim = sim
                top_opp = opp
                top_opp.flash_provider = sim.flash_provider
        except Exception as exc:
            logger.error("simulate_arb failed for %s: %s", opp.pair, exc)

    # Execute only the #1 simulated opportunity
    if top_opp and top_sim:
        try:
            execute, reason = should_execute(top_opp, top_sim)
            if execute:
                result = execute_arb(top_opp, top_sim)
                if not result["success"]:
                    log_opportunity(top_opp, "DRY", top_sim)
            else:
                log_opportunity(top_opp, "SKIP", top_sim)
                logger.debug("SKIP: %s", reason)
        except Exception as exc:
            logger.error("Execution logic failed: %s", exc)

    if stats is not None:
        stats.record_cycle(all_opps, top_sim)


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    _setup_logging()

    try:
        config.validate()
    except ValueError as exc:
        logger.critical("Config validation failed: %s", exc)
        sys.exit(1)

    w3_read = Web3(Web3.HTTPProvider(config.BASE_RPC_URL))
    if not w3_read.is_connected():
        logger.critical("Cannot connect to CDP RPC: %s", config.BASE_RPC_URL)
        sys.exit(1)

    logger.info("Connected to Base mainnet — block %d", w3_read.eth.block_number)
    _print_banner()

    stats = CycleStats()
    cycle = 0

    while True:
        cycle += 1
        t0 = time.time()
        try:
            run_cycle(w3_read, stats)
        except Exception as exc:
            logger.error("Unhandled cycle error (cycle=%d): %s", cycle, exc)

        # Periodic stats emission
        if cycle % 100 == 0:
            stats.emit_stats()
        if cycle % 1000 == 0:
            stats.emit_pair_stats()

        elapsed = time.time() - t0
        sleep_time = max(0.0, config.SCAN_INTERVAL_SECONDS - elapsed)
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()
