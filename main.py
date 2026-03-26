"""
DEX Arbitrage Bot — main scan loop.
Synchronous polling loop. Connects to Base mainnet via CDP RPC for reads,
Alchemy for execution (when enabled). Never uses Alchemy for price reads.
Never imports from the morpho_scanner liquidation bot.
"""

import logging
import sys
import time
from pathlib import Path
from datetime import datetime, timezone

from web3 import Web3

import config
from price_scanner import get_all_prices
from arb_detector import detect_opportunity
from executor import should_execute, log_opportunity, execute_arb

# ── Logging setup ─────────────────────────────────────────────────────────────

def _setup_logging() -> None:
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
    print(
        f"\n{'='*65}\n"
        f"  ARB_BOT | pair=cbBTC/USDC,weETH/WETH\n"
        f"  mode={mode} | min_spread={config.MIN_SPREAD_PCT:.2f}%\n"
        f"  min_profit=${config.MIN_NET_PROFIT_USD:.2f} | interval={config.SCAN_INTERVAL_SECONDS}s\n"
        f"  wallet={config.WALLET_ADDRESS}\n"
        f"  contract={'(not deployed)' if not config.ARB_EXECUTOR_ADDRESS else config.ARB_EXECUTOR_ADDRESS}\n"
        f"{'='*65}\n"
    )


# ── Per-cycle scan ────────────────────────────────────────────────────────────

def _log_scan_line(pair: str, aero_price: float, uni_price: float,
                   gross_pct: float, net_pct: float,
                   profit: float, is_profitable: bool) -> None:
    tag = "PROFITABLE ✅" if is_profitable else "BELOW_THRESHOLD ❌"
    print(
        f"ARB_SCAN | {pair} | "
        f"aero={aero_price:.2f} uni={uni_price:.2f} | "
        f"spread={gross_pct:.4f}% net={net_pct:.4f}% | "
        f"profit=${profit:.2f} | {tag}"
    )


def run_cycle(w3_read: Web3) -> None:
    """Execute one scan cycle — fetch prices, detect opportunity, log results."""
    try:
        prices = get_all_prices(w3_read)
    except Exception as exc:
        logger.error("Price fetch failed: %s", exc)
        return

    # Log scan line for each pair
    for pair, (aero_quote, uni_quote) in prices.items():
        try:
            aero_price  = aero_quote.price
            uni_price   = uni_quote.price
            gross_pct   = abs(aero_price - uni_price) / min(aero_price, uni_price) * 100
            total_fee   = (aero_quote.fee_pct + uni_quote.fee_pct) * 100
            net_pct     = gross_pct - total_fee
            is_above    = net_pct >= config.MIN_SPREAD_PCT
            profit_est  = (min(34_000.0, config.MAX_FLASH_LOAN_USDC) * net_pct / 100
                           if is_above else 0.0)
            _log_scan_line(
                pair=pair,
                aero_price=aero_price,
                uni_price=uni_price,
                gross_pct=gross_pct,
                net_pct=net_pct,
                profit=profit_est,
                is_profitable=is_above,
            )
        except Exception as exc:
            logger.error("Scan line for %s failed: %s", pair, exc)

    # Detect best opportunity
    try:
        opp = detect_opportunity(
            prices=prices,
            min_spread_pct=config.MIN_SPREAD_PCT,
            max_flash_usdc=config.MAX_FLASH_LOAN_USDC,
        )
    except Exception as exc:
        logger.error("detect_opportunity failed: %s", exc)
        return

    if opp is None:
        return

    # Log it
    try:
        if opp.is_profitable and opp.estimated_profit_usdc >= config.MIN_NET_PROFIT_USD:
            log_opportunity(opp, "PROFITABLE")

            # Check execution gates
            execute, reason = should_execute(opp)
            if execute:
                result = execute_arb(opp)
                if not result["success"]:
                    log_opportunity(opp, "DRY")
            else:
                log_opportunity(opp, "SKIP")
                logger.debug("SKIP: %s", reason)
        else:
            log_opportunity(opp, "BELOW_THRESHOLD")
    except Exception as exc:
        logger.error("Opportunity logging/execution failed: %s", exc)


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    _setup_logging()

    try:
        config.validate()
    except ValueError as exc:
        logger.critical("Config validation failed: %s", exc)
        sys.exit(1)

    # Connect — CDP for reads, Alchemy for execution
    w3_read = Web3(Web3.HTTPProvider(config.BASE_RPC_URL))
    if not w3_read.is_connected():
        logger.critical("Cannot connect to CDP RPC: %s", config.BASE_RPC_URL)
        sys.exit(1)

    logger.info("Connected to Base mainnet — block %d", w3_read.eth.block_number)

    _print_banner()

    cycle = 0
    while True:
        cycle += 1
        t0 = time.time()
        try:
            run_cycle(w3_read)
        except Exception as exc:
            # Never crash the main loop
            logger.error("Unhandled cycle error (cycle=%d): %s", cycle, exc)

        elapsed = time.time() - t0
        sleep_time = max(0.0, config.SCAN_INTERVAL_SECONDS - elapsed)
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()
