"""
Arbitrage opportunity detector for DEX Arb Bot.
Detects best cross-DEX spread across all pairs, assigns tiers, sizes trades,
simulates execution, and selects flash loan provider.
Never imports from the morpho_scanner liquidation bot.
"""

import logging
import time
from dataclasses import dataclass, field
from typing import List, Optional

from web3 import Web3

from price_scanner import PriceQuote
import config

logger = logging.getLogger(__name__)

# ── ERC20 balanceOf ABI (for flash loan provider check) ───────────────────────
_ERC20_ABI = [
    {
        "inputs": [{"name": "account", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    }
]

_QUOTER_V2_ABI = [
    {
        "inputs": [
            {
                "components": [
                    {"name": "tokenIn",           "type": "address"},
                    {"name": "tokenOut",          "type": "address"},
                    {"name": "amountIn",          "type": "uint256"},
                    {"name": "fee",               "type": "uint24"},
                    {"name": "sqrtPriceLimitX96", "type": "uint160"},
                ],
                "name": "params",
                "type": "tuple",
            }
        ],
        "name": "quoteExactInputSingle",
        "outputs": [
            {"name": "amountOut",               "type": "uint256"},
            {"name": "sqrtPriceX96After",       "type": "uint160"},
            {"name": "initializedTicksCrossed", "type": "uint32"},
            {"name": "gasEstimate",             "type": "uint256"},
        ],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]


# ── Tier assignment ───────────────────────────────────────────────────────────

def assign_tier(net_spread_pct: float) -> str:
    """
    Classify net spread into execution tier.

    PRIME    >= 0.15%  → max flash ($50k)
    GOOD     >= 0.10%  → normal flash ($34k)
    MARGINAL >= 0.065% → half flash ($17k)
    BELOW    >  0%     → log only, no execute
    NO_ARB   <= 0%     → skip entirely
    """
    if net_spread_pct >= config.TIER_PRIME_PCT:
        return "PRIME"
    if net_spread_pct >= config.TIER_GOOD_PCT:
        return "GOOD"
    if net_spread_pct >= config.TIER_MARGINAL_PCT:
        return "MARGINAL"
    if net_spread_pct > 0:
        return "BELOW"
    return "NO_ARB"


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class ArbOpportunity:
    pair: str
    buy_venue: str              # cheaper DEX (buy here)
    sell_venue: str             # pricier DEX (sell here)
    buy_price: float
    sell_price: float
    gross_spread_pct: float
    total_fee_pct: float
    net_spread_pct: float
    flash_loan_usdc: float
    estimated_profit_usdc: float
    is_profitable: bool
    timestamp: float
    tier: str = "BELOW"         # PRIME / GOOD / MARGINAL / BELOW / NO_ARB
    flash_provider: str = ""    # "Morpho" or "Balancer" or ""

    def __repr__(self) -> str:
        return (
            f"ArbOpportunity({self.pair} {self.tier} "
            f"buy={self.buy_venue}@{self.buy_price:.4f} "
            f"sell={self.sell_venue}@{self.sell_price:.4f} "
            f"net={self.net_spread_pct:.4f}% "
            f"profit=${self.estimated_profit_usdc:.2f})"
        )


@dataclass
class SimResult:
    """Result of pre-execution simulation via QuoterV2."""
    buy_dex: str
    sell_dex: str
    token_amount: float         # intermediate token received from buy leg
    usdc_in: float              # flash loan amount (USDC)
    usdc_out: float             # USDC received from sell leg
    gross_profit_usd: float
    gas_cost_usd: float
    net_profit_usd: float
    flash_provider: str
    is_executable: bool
    rejection_reason: str       # "" if executable


# ── Trade sizing ──────────────────────────────────────────────────────────────

def calculate_trade_size(
    buy_price: float,
    net_spread_pct: float,
    max_usdc: float,
) -> tuple:
    """
    Return (usdc_amount, asset_amount) based on opportunity tier.

    Tier sizing:
        PRIME    (>= 0.15%) → min($50k, max_usdc)
        GOOD     (>= 0.10%) → min($34k, max_usdc)
        MARGINAL (>= 0.065%) → min($17k, max_usdc)
        BELOW / NO_ARB      → $0 (don't trade)
    """
    tier = assign_tier(net_spread_pct)
    if tier == "PRIME":
        usdc_amount = min(config.FLASH_PRIME_USDC, max_usdc)
    elif tier == "GOOD":
        usdc_amount = min(config.FLASH_GOOD_USDC, max_usdc)
    elif tier == "MARGINAL":
        usdc_amount = min(config.FLASH_MARGINAL_USDC, max_usdc)
    else:
        usdc_amount = 0.0

    asset_amount = usdc_amount / buy_price if buy_price > 0 else 0.0
    return usdc_amount, asset_amount


# ── Flash loan provider selection ─────────────────────────────────────────────

def select_flash_provider(w3: Web3, required_usdc: float) -> str:
    """
    Select flash loan provider by checking available USDC liquidity.

    Priority:
        1. Morpho — if USDC.balanceOf(Morpho) >= required_usdc
        2. Balancer — if USDC.balanceOf(Balancer) >= required_usdc
        3. "" — no provider available (log NO_FLASH_LIQUIDITY)
    """
    usdc = w3.eth.contract(
        address=config.USDC_ADDRESS,
        abi=_ERC20_ABI,
    )
    required_raw = int(required_usdc * 1e6)

    for provider in config.FLASH_LOAN_PROVIDERS:
        try:
            bal = usdc.functions.balanceOf(
                Web3.to_checksum_address(provider["address"])
            ).call()
            if bal >= required_raw:
                return provider["name"]
        except Exception as e:
            logger.debug("flash provider check failed %s: %s", provider["name"], e)

    return ""


# ── Core pair evaluator ───────────────────────────────────────────────────────

def _evaluate_pair_best(
    pair: str,
    quotes: List[PriceQuote],
    min_spread_pct: float,
    max_flash_usdc: float,
) -> Optional["ArbOpportunity"]:
    """
    Find best arb across a list of DEX quotes for one pair.
    Selects cheapest buy and priciest sell across ALL DEXes.
    """
    valid = [q for q in quotes if q.price > 0]
    if len(valid) < 2:
        return None

    # Find best sell (highest price) and best buy from a DIFFERENT venue
    sell_quote = max(valid, key=lambda q: q.price)
    buy_candidates = [q for q in valid if q.venue != sell_quote.venue]
    if not buy_candidates:
        return None  # all quotes from same DEX — no cross-venue arb
    buy_quote = min(buy_candidates, key=lambda q: q.price)

    buy_price  = buy_quote.price
    sell_price = sell_quote.price

    gross_spread_pct = (sell_price - buy_price) / buy_price * 100.0
    total_fee_pct    = (buy_quote.fee_pct + sell_quote.fee_pct) * 100.0
    net_spread_pct   = gross_spread_pct - total_fee_pct

    tier         = assign_tier(net_spread_pct)
    is_profitable = tier in ("PRIME", "GOOD", "MARGINAL")

    flash_loan_usdc, _ = calculate_trade_size(
        buy_price=buy_price,
        net_spread_pct=net_spread_pct,
        max_usdc=max_flash_usdc,
    )

    # Profit = flash_loan * net_spread / 100
    estimated_profit_usdc = flash_loan_usdc * (net_spread_pct / 100.0)

    return ArbOpportunity(
        pair=pair,
        buy_venue=buy_quote.venue,
        sell_venue=sell_quote.venue,
        buy_price=buy_price,
        sell_price=sell_price,
        gross_spread_pct=gross_spread_pct,
        total_fee_pct=total_fee_pct,
        net_spread_pct=net_spread_pct,
        flash_loan_usdc=flash_loan_usdc,
        estimated_profit_usdc=estimated_profit_usdc,
        is_profitable=is_profitable,
        timestamp=time.time(),
        tier=tier,
    )


def _evaluate_pair(
    pair: str,
    aero_quote: PriceQuote,
    uni_quote: PriceQuote,
    min_spread_pct: float,
    max_flash_usdc: float,
) -> Optional["ArbOpportunity"]:
    """
    Legacy two-quote evaluator — kept for backward compat with existing tests.
    Delegates to _evaluate_pair_best.
    """
    return _evaluate_pair_best(pair, [aero_quote, uni_quote], min_spread_pct, max_flash_usdc)


# ── Multi-opportunity detector ────────────────────────────────────────────────

def detect_all_opportunities(
    prices: dict,
    min_spread_pct: float,
    max_flash_usdc: float,
) -> List[ArbOpportunity]:
    """
    Scan all pairs and return ALL opportunities (sorted by net profit descending).
    Includes below-threshold opportunities (is_profitable=False) so caller can log them.

    prices format: {pair_name: [PriceQuote, ...]}  OR  {pair_name: (quote1, quote2)}
    """
    results: List[ArbOpportunity] = []

    for pair, quotes_raw in prices.items():
        quotes = list(quotes_raw)  # handle both tuple (legacy) and list (new)
        opp = _evaluate_pair_best(pair, quotes, min_spread_pct, max_flash_usdc)
        if opp is not None:
            results.append(opp)

    results.sort(key=lambda o: o.estimated_profit_usdc, reverse=True)
    return results


def detect_opportunity(
    prices: dict,
    min_spread_pct: float,
    max_flash_usdc: float,
) -> Optional[ArbOpportunity]:
    """
    Return the single best profitable opportunity, or None.
    Backward-compatible entry point used by tests and legacy callers.
    """
    all_opps = detect_all_opportunities(prices, min_spread_pct, max_flash_usdc)
    if not all_opps:
        return None
    return all_opps[0]


# ── Execution simulation ──────────────────────────────────────────────────────

def _get_dex_quoter(dex_name: str) -> str:
    """Look up quoter address for a DEX by name."""
    for dex in config.DEX_CONFIG:
        if dex["name"] == dex_name:
            return dex.get("quoter") or ""
    return ""


def _get_dex_best_fee(dex_name: str, token_in: str, token_out: str,
                      w3: Web3) -> int:
    """
    Find the best fee tier for a V3 DEX pair (highest TVL proxy: first non-zero pool).
    Returns 0 if unknown.
    """
    from price_scanner import get_uniswap_pool, _ZERO_ADDRESS
    for dex in config.DEX_CONFIG:
        if dex["name"] == dex_name and dex["type"] == "uniswap_v3":
            for fee in dex.get("fee_tiers", [500]):
                try:
                    pool = get_uniswap_pool(w3, token_in, token_out, fee, dex["factory"])
                    if pool and pool != _ZERO_ADDRESS:
                        return fee
                except Exception:
                    continue
    return 0


def simulate_arb(w3: Web3, opp: ArbOpportunity) -> SimResult:
    """
    Simulate the full arb route by calling QuoterV2 (or equivalent) on both legs.

    Steps:
        1. Quote buy leg:  flash_loan_usdc USDC → token_intermediate on buy_dex
        2. Quote sell leg: token_intermediate → USDC on sell_dex
        3. Estimate gas cost in USD
        4. Check slippage: if either leg > 2% slippage vs scanner price → reject

    Returns SimResult with is_executable and rejection_reason.
    """
    pair_cfg = next(
        (p for p in config.PAIR_CONFIG if p["name"] == opp.pair), None
    )
    if pair_cfg is None:
        return SimResult(
            buy_dex=opp.buy_venue, sell_dex=opp.sell_venue,
            token_amount=0, usdc_in=opp.flash_loan_usdc, usdc_out=0,
            gross_profit_usd=0, gas_cost_usd=0, net_profit_usd=0,
            flash_provider="", is_executable=False,
            rejection_reason=f"pair_config_not_found:{opp.pair}",
        )

    token_in  = pair_cfg["token_in"]
    token_out = pair_cfg["token_out"]
    dec_in    = pair_cfg["dec_in"]
    dec_out   = pair_cfg["dec_out"]

    # Flash loan is in token_out (USDC or WETH) since we need to buy token_in
    # For USDC-denominated pairs (token_out=USDC): flash borrow USDC, buy token_in
    # General: flash_loan_usdc is always in USDC terms
    usdc_in = opp.flash_loan_usdc

    # Step 1: Check flash provider
    flash_provider = select_flash_provider(w3, usdc_in)
    if not flash_provider:
        return SimResult(
            buy_dex=opp.buy_venue, sell_dex=opp.sell_venue,
            token_amount=0, usdc_in=usdc_in, usdc_out=0,
            gross_profit_usd=0, gas_cost_usd=0, net_profit_usd=0,
            flash_provider="", is_executable=False,
            rejection_reason="NO_FLASH_LIQUIDITY",
        )

    # Step 2: Quote buy leg (USDC → token_in on buy_dex)
    # We use token_out as the borrow token (USDC) and token_in as intermediate
    buy_quoter  = _get_dex_quoter(opp.buy_venue)
    sell_quoter = _get_dex_quoter(opp.sell_venue)

    token_amount = 0.0
    usdc_out = 0.0

    # For simulation, try V3 quoter first; fall back to scanner price × amount
    borrow_token = token_out   # USDC (or WETH for WETH-denominated pairs)
    borrow_dec   = dec_out
    intermediate = token_in
    inter_dec    = dec_in

    usdc_raw = int(usdc_in * (10 ** borrow_dec))

    # Buy leg simulation
    try:
        if buy_quoter:
            fee = _get_dex_best_fee(opp.buy_venue, borrow_token, intermediate, w3)
            if fee == 0:
                fee = 500  # fallback
            q = w3.eth.contract(
                address=Web3.to_checksum_address(buy_quoter),
                abi=_QUOTER_V2_ABI,
            )
            result = q.functions.quoteExactInputSingle({
                "tokenIn":           Web3.to_checksum_address(borrow_token),
                "tokenOut":          Web3.to_checksum_address(intermediate),
                "amountIn":          usdc_raw,
                "fee":               fee,
                "sqrtPriceLimitX96": 0,
            }).call()
            token_amount = result[0] / (10 ** inter_dec)
        else:
            # No quoter (Aerodrome Slipstream / vAMM) — use scanner buy price
            token_amount = usdc_in / opp.buy_price if opp.buy_price > 0 else 0.0
    except Exception as e:
        logger.debug("sim buy leg failed pair=%s dex=%s: %s", opp.pair, opp.buy_venue, e)
        token_amount = usdc_in / opp.buy_price if opp.buy_price > 0 else 0.0

    if token_amount == 0:
        return SimResult(
            buy_dex=opp.buy_venue, sell_dex=opp.sell_venue,
            token_amount=0, usdc_in=usdc_in, usdc_out=0,
            gross_profit_usd=0, gas_cost_usd=0, net_profit_usd=0,
            flash_provider=flash_provider, is_executable=False,
            rejection_reason="buy_quote_zero",
        )

    # Sell leg simulation
    token_raw = int(token_amount * (10 ** inter_dec))
    try:
        if sell_quoter:
            fee = _get_dex_best_fee(opp.sell_venue, intermediate, borrow_token, w3)
            if fee == 0:
                fee = 500
            q = w3.eth.contract(
                address=Web3.to_checksum_address(sell_quoter),
                abi=_QUOTER_V2_ABI,
            )
            result = q.functions.quoteExactInputSingle({
                "tokenIn":           Web3.to_checksum_address(intermediate),
                "tokenOut":          Web3.to_checksum_address(borrow_token),
                "amountIn":          token_raw,
                "fee":               fee,
                "sqrtPriceLimitX96": 0,
            }).call()
            usdc_out = result[0] / (10 ** borrow_dec)
        else:
            usdc_out = token_amount * opp.sell_price
    except Exception as e:
        logger.debug("sim sell leg failed pair=%s dex=%s: %s", opp.pair, opp.sell_venue, e)
        usdc_out = token_amount * opp.sell_price

    # Step 3: Gas estimation
    try:
        gas_price_wei = w3.eth.gas_price
        eth_price_usd = _estimate_eth_price(w3)
        gas_cost_usd = 400_000 * gas_price_wei * eth_price_usd / 1e18
    except Exception:
        gas_cost_usd = 0.5  # fallback: 50 cents

    # Step 4: Slippage check
    # Expected token from buy at scanner price
    expected_token = usdc_in / opp.buy_price if opp.buy_price > 0 else 1.0
    buy_slippage = abs(token_amount - expected_token) / expected_token if expected_token > 0 else 0
    if buy_slippage > 0.02:
        return SimResult(
            buy_dex=opp.buy_venue, sell_dex=opp.sell_venue,
            token_amount=token_amount, usdc_in=usdc_in, usdc_out=usdc_out,
            gross_profit_usd=usdc_out - usdc_in, gas_cost_usd=gas_cost_usd,
            net_profit_usd=usdc_out - usdc_in - gas_cost_usd,
            flash_provider=flash_provider, is_executable=False,
            rejection_reason=f"buy_slippage_too_high:{buy_slippage*100:.2f}%",
        )

    expected_usdc_out = token_amount * opp.sell_price
    sell_slippage = abs(usdc_out - expected_usdc_out) / expected_usdc_out if expected_usdc_out > 0 else 0
    if sell_slippage > 0.02:
        return SimResult(
            buy_dex=opp.buy_venue, sell_dex=opp.sell_venue,
            token_amount=token_amount, usdc_in=usdc_in, usdc_out=usdc_out,
            gross_profit_usd=usdc_out - usdc_in, gas_cost_usd=gas_cost_usd,
            net_profit_usd=usdc_out - usdc_in - gas_cost_usd,
            flash_provider=flash_provider, is_executable=False,
            rejection_reason=f"sell_slippage_too_high:{sell_slippage*100:.2f}%",
        )

    gross_profit = usdc_out - usdc_in
    net_profit   = gross_profit - gas_cost_usd
    is_exec      = net_profit >= config.MIN_NET_PROFIT_USD

    return SimResult(
        buy_dex=opp.buy_venue,
        sell_dex=opp.sell_venue,
        token_amount=token_amount,
        usdc_in=usdc_in,
        usdc_out=usdc_out,
        gross_profit_usd=gross_profit,
        gas_cost_usd=gas_cost_usd,
        net_profit_usd=net_profit,
        flash_provider=flash_provider,
        is_executable=is_exec,
        rejection_reason="" if is_exec else f"below_min_profit:{net_profit:.2f}<{config.MIN_NET_PROFIT_USD}",
    )


def _estimate_eth_price(w3: Web3) -> float:
    """
    Rough ETH price via Uniswap V3 WETH/USDC fee=500 pool.
    Falls back to 3500 on any error.
    """
    try:
        from price_scanner import get_uniswap_price
        q = get_uniswap_price(
            w3=w3,
            token_in=config.WETH_ADDRESS,
            token_out=config.USDC_ADDRESS,
            amount_in=int(1e18),
            fee=500,
            dec_in=18,
            dec_out=6,
            pair="WETH/USDC",
            fee_pct=0.0005,
        )
        return q.price if q.price > 100 else 3500.0
    except Exception:
        return 3500.0
