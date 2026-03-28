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

# ── Cost breakdown ────────────────────────────────────────────────────────────

@dataclass
class CostBreakdown:
    """Full cost decomposition for one arb opportunity."""
    gross_spread_pct: float        # raw price difference (sell - buy) / buy
    lp_fee_buy_pct: float          # LP fee on buy leg (e.g. 0.05 for 0.05%)
    lp_fee_sell_pct: float         # LP fee on sell leg
    price_impact_buy_pct: float    # slippage at flash size on buy leg (0 at scan time)
    price_impact_sell_pct: float   # slippage at flash size on sell leg (0 at scan time)
    gas_pct: float                 # estimated gas as % of notional
    flash_fee_pct: float           # flash loan fee (0% Morpho, 0% Balancer)
    net_spread_pct: float          # gross minus all costs
    net_profit_usdc: float         # net_spread * notional / 100


def compute_cost_breakdown(
    gross_spread_pct: float,
    lp_fee_buy_pct: float,
    lp_fee_sell_pct: float,
    price_impact_buy_pct: float,
    price_impact_sell_pct: float,
    flash_loan_usdc: float,
    gas_cost_usd: float,
) -> CostBreakdown:
    """Compute full cost breakdown. All percentage inputs are in percent (not fractions)."""
    gas_pct = (gas_cost_usd / flash_loan_usdc * 100.0) if flash_loan_usdc > 0 else 0.0
    flash_fee_pct = 0.0  # Morpho and Balancer are both 0%
    net = (gross_spread_pct
           - lp_fee_buy_pct - lp_fee_sell_pct
           - price_impact_buy_pct - price_impact_sell_pct
           - gas_pct - flash_fee_pct)
    return CostBreakdown(
        gross_spread_pct=gross_spread_pct,
        lp_fee_buy_pct=lp_fee_buy_pct,
        lp_fee_sell_pct=lp_fee_sell_pct,
        price_impact_buy_pct=price_impact_buy_pct,
        price_impact_sell_pct=price_impact_sell_pct,
        gas_pct=gas_pct,
        flash_fee_pct=flash_fee_pct,
        net_spread_pct=net,
        net_profit_usdc=flash_loan_usdc * net / 100.0,
    )


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
    tier: str = "BELOW"         # PRIME / GOOD / MARGINAL / BELOW / NO_ARB / DEPTH_REJECTED
    flash_provider: str = ""    # "Morpho" or "Balancer" or ""
    cost: Optional[CostBreakdown] = None  # full cost breakdown (None when no w3 available)

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


# ── Depth discovery ───────────────────────────────────────────────────────────

def _get_dex_cfg(venue_name: str) -> Optional[dict]:
    """Return DEX_CONFIG entry matching venue_name, or None."""
    for dex in config.DEX_CONFIG:
        if dex["name"] == venue_name:
            return dex
    return None


def find_max_executable_size(
    w3: Web3,
    pair_cfg: dict,
    buy_dex_cfg: dict,
    sell_dex_cfg: dict,
    max_usdc: float,
    slippage_tol: float,
    buy_price: float,
) -> float:
    """
    Step-down ladder to find the largest flash loan size (USD) where both the
    buy and sell legs stay within slippage_tol.

    Probes: max_usdc × [1.0, 0.5, 0.25, 0.1, 0.05]
    Returns the first size that passes, or 0.0 if none do.

    buy_price: token_out per token_in at unit_size (from scanner).
    For WETH-denominated pairs, buy_price is in WETH/token.

    All quoter calls are batched via multicall3 (one round-trip for all probes
    instead of 10+ sequential calls). Uses w3_read (dRPC) only.
    """
    from price_scanner import (
        quote_at_amount,
        build_depth_probe_calldata,
        decode_depth_probe_result,
    )
    from utils.multicall import multicall3

    unit_size = pair_cfg["unit_size"]
    token_out = pair_cfg["token_out"]
    is_weth_pair = token_out.lower() == config.WETH_ADDRESS.lower()

    # Rough ETH price for USD↔WETH conversion (only needed for WETH pairs)
    eth_price = 3500.0
    if is_weth_pair:
        try:
            eth_price = _estimate_eth_price(w3)
        except Exception:
            pass

    # Reference price at 1% of unit_size (tiny trade, essentially no impact)
    ref_amount = max(unit_size * 0.01, 10 ** (-pair_cfg["dec_in"]))

    # ── Phase 1: batch the 2 reference calls + (5 × 2) probe calls = 12 calls ──
    # Build calldata for reference calls
    probe_specs: list = []  # list of (label, amount_in_human, dex_side)
    mc_calls: list = []     # list of {target, callData}
    mc_meta: list = []      # parallel: (label, call_type, dec_out, amount_in_human)

    for side, dex_cfg_arg, label in [
        ("buy_ref",  buy_dex_cfg,  "buy_ref"),
        ("sell_ref", sell_dex_cfg, "sell_ref"),
    ]:
        info = build_depth_probe_calldata(pair_cfg, dex_cfg_arg, ref_amount)
        if info is None:
            # Pool not cached — fall back to sequential quote_at_amount for ref
            buy_ref  = quote_at_amount(w3, pair_cfg, buy_dex_cfg,  ref_amount)
            sell_ref = quote_at_amount(w3, pair_cfg, sell_dex_cfg, ref_amount)
            if not buy_ref or not sell_ref:
                return 0.0
            # Build probe calls sequentially (fallback path)
            if is_weth_pair:
                max_token_in = (max_usdc / eth_price) / buy_ref
            else:
                max_token_in = max_usdc / buy_ref
            for frac in [1.0, 0.5, 0.25, 0.1, 0.05]:
                test_token_in = max_token_in * frac
                buy_a  = quote_at_amount(w3, pair_cfg, buy_dex_cfg,  test_token_in)
                sell_a = quote_at_amount(w3, pair_cfg, sell_dex_cfg, test_token_in)
                if not buy_a or not sell_a:
                    continue
                buy_slip  = abs(buy_ref - buy_a)  / buy_ref  if buy_ref  > 0 else 1.0
                sell_slip = abs(sell_ref - sell_a) / sell_ref if sell_ref > 0 else 1.0
                if buy_slip <= slippage_tol and sell_slip <= slippage_tol:
                    if is_weth_pair:
                        return test_token_in * buy_ref * eth_price
                    else:
                        return test_token_in * buy_ref
            return 0.0
        target, cd, call_type, dec_out, _ = info
        mc_calls.append({"target": target, "callData": cd})
        mc_meta.append((label, call_type, dec_out, ref_amount))

    # Build probe-size calldata (5 fractions × 2 legs = 10 calls)
    # We need the reference prices first to compute max_token_in, but since
    # we don't know buy_ref yet, we must compute token-in amounts later.
    # Strategy: batch ref calls first (2 calls), decode, then batch all probe calls.
    # Two-phase batching: phase-1 = 2 ref calls, phase-2 = 10 probe calls.

    # ── Phase 1: 2 ref calls in one multicall3 round-trip ─────────────────────
    CHUNK_SIZE = 50
    try:
        ref_raw = multicall3(w3, mc_calls)
    except Exception as exc:
        logger.debug("depth probe ref multicall failed: %s", exc)
        return 0.0

    buy_ref = decode_depth_probe_result(
        ref_raw[0], mc_meta[0][1], mc_meta[0][2], ref_amount
    )
    sell_ref = decode_depth_probe_result(
        ref_raw[1], mc_meta[1][1], mc_meta[1][2], ref_amount
    )
    if not buy_ref or not sell_ref:
        return 0.0

    # Convert max_usdc (USD) to token_in using reference buy price
    if is_weth_pair:
        max_token_in = (max_usdc / eth_price) / buy_ref
    else:
        max_token_in = max_usdc / buy_ref

    # ── Phase 2: batch all 10 probe calls (5 fracs × 2 legs) ──────────────────
    probe_fracs = [1.0, 0.5, 0.25, 0.1, 0.05]
    probe_calls: list = []
    probe_meta: list = []   # (frac_idx, leg: "buy"|"sell", call_type, dec_out, amount)

    for frac_idx, frac in enumerate(probe_fracs):
        test_token_in = max_token_in * frac
        for leg, dex_cfg_arg in [("buy", buy_dex_cfg), ("sell", sell_dex_cfg)]:
            info = build_depth_probe_calldata(pair_cfg, dex_cfg_arg, test_token_in)
            if info is None:
                # No pool cached — insert a placeholder (None result = skip)
                probe_calls.append(None)
                probe_meta.append((frac_idx, leg, None, None, test_token_in))
            else:
                target, cd, call_type, dec_out, amt = info
                probe_calls.append({"target": target, "callData": cd})
                probe_meta.append((frac_idx, leg, call_type, dec_out, test_token_in))

    # Execute in chunks of CHUNK_SIZE (handles >50 probes gracefully)
    valid_calls = [(i, c) for i, c in enumerate(probe_calls) if c is not None]
    raw_results: dict = {}  # index → decoded price or None

    for chunk_start in range(0, len(valid_calls), CHUNK_SIZE):
        chunk = valid_calls[chunk_start: chunk_start + CHUNK_SIZE]
        try:
            chunk_raw = multicall3(w3, [c for _, c in chunk])
        except Exception as exc:
            logger.debug("depth probe batch failed: %s", exc)
            chunk_raw = [None] * len(chunk)
        for (orig_idx, _), raw in zip(chunk, chunk_raw):
            _, leg, call_type, dec_out, amt = probe_meta[orig_idx]
            if call_type is None:
                raw_results[orig_idx] = None
            else:
                raw_results[orig_idx] = decode_depth_probe_result(raw, call_type, dec_out, amt)

    # Mark placeholder (None-call) entries as None
    for i, c in enumerate(probe_calls):
        if c is None and i not in raw_results:
            raw_results[i] = None

    # ── Phase 3: evaluate probe sizes in order (largest first) ────────────────
    # Collect per-frac results
    frac_prices: dict = {}  # frac_idx → {"buy": price|None, "sell": price|None}
    for i, (frac_idx, leg, _, _, _) in enumerate(probe_meta):
        if frac_idx not in frac_prices:
            frac_prices[frac_idx] = {"buy": None, "sell": None}
        frac_prices[frac_idx][leg] = raw_results.get(i)

    for frac_idx, frac in enumerate(probe_fracs):
        prices = frac_prices.get(frac_idx, {})
        buy_actual  = prices.get("buy")
        sell_actual = prices.get("sell")
        if not buy_actual or not sell_actual:
            continue

        buy_slip  = abs(buy_ref  - buy_actual)  / buy_ref  if buy_ref  > 0 else 1.0
        sell_slip = abs(sell_ref - sell_actual) / sell_ref if sell_ref > 0 else 1.0

        if buy_slip <= slippage_tol and sell_slip <= slippage_tol:
            test_token_in = max_token_in * frac
            if is_weth_pair:
                return test_token_in * buy_ref * eth_price
            else:
                return test_token_in * buy_ref

    return 0.0  # No size within tolerance


# ── Core pair evaluator ───────────────────────────────────────────────────────

def _evaluate_pair_best(
    pair: str,
    quotes: List[PriceQuote],
    min_spread_pct: float,
    max_flash_usdc: float,
    w3: Optional[Web3] = None,
) -> Optional["ArbOpportunity"]:
    """
    Find best arb across a list of DEX quotes for one pair.
    Selects cheapest buy and priciest sell across ALL DEXes.

    When w3 is provided, runs depth discovery via find_max_executable_size
    before assigning flash_loan_usdc. Profitable opps that cannot be sized
    within MAX_SLIPPAGE_PER_LEG at any test level return tier=DEPTH_REJECTED.
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

    # Sanity cap: reject absurdly large spreads before tier assignment.
    if gross_spread_pct > config.MAX_GROSS_SPREAD_PCT:
        logger.debug(
            "OUTLIER_REJECTED | %s | gross_spread=%.4f%% > sanity_cap=%.1f%% | "
            "buy=%s@%.8g sell=%s@%.8g — likely stale/thin pool vs slippage mismatch",
            pair, gross_spread_pct, config.MAX_GROSS_SPREAD_PCT,
            buy_quote.venue, buy_price, sell_quote.venue, sell_price,
        )
        return None

    tier          = assign_tier(net_spread_pct)
    is_profitable = tier in ("PRIME", "GOOD", "MARGINAL")

    flash_loan_usdc, _ = calculate_trade_size(
        buy_price=buy_price,
        net_spread_pct=net_spread_pct,
        max_usdc=max_flash_usdc,
    )

    # Compute cost breakdown at scan size (price impact ≈ 0 at unit_size)
    # lp_fee_pct stored as fraction → convert to percent for CostBreakdown
    cost = compute_cost_breakdown(
        gross_spread_pct=gross_spread_pct,
        lp_fee_buy_pct=buy_quote.fee_pct * 100.0,
        lp_fee_sell_pct=sell_quote.fee_pct * 100.0,
        price_impact_buy_pct=0.0,   # unknown at scan time (unit_size is tiny)
        price_impact_sell_pct=0.0,
        flash_loan_usdc=flash_loan_usdc,
        gas_cost_usd=0.50,          # conservative $0.50 gas estimate
    )

    # Depth discovery: probe pool depth when w3 available and opp is profitable
    if is_profitable and w3 is not None:
        pair_cfg     = next((p for p in config.PAIR_CONFIG if p["name"] == pair), None)
        buy_dex_cfg  = _get_dex_cfg(buy_quote.venue)
        sell_dex_cfg = _get_dex_cfg(sell_quote.venue)

        if pair_cfg and buy_dex_cfg and sell_dex_cfg:
            max_size = find_max_executable_size(
                w3=w3,
                pair_cfg=pair_cfg,
                buy_dex_cfg=buy_dex_cfg,
                sell_dex_cfg=sell_dex_cfg,
                max_usdc=max_flash_usdc,
                slippage_tol=config.MAX_SLIPPAGE_PER_LEG,
                buy_price=buy_price,
            )
            if max_size == 0.0:
                logger.debug(
                    "DEPTH_REJECTED | %s | buy=%s sell=%s | "
                    "no executable size within %.0f%% slippage",
                    pair, buy_quote.venue, sell_quote.venue,
                    config.MAX_SLIPPAGE_PER_LEG * 100,
                )
                return ArbOpportunity(
                    pair=pair,
                    buy_venue=buy_quote.venue,
                    sell_venue=sell_quote.venue,
                    buy_price=buy_price,
                    sell_price=sell_price,
                    gross_spread_pct=gross_spread_pct,
                    total_fee_pct=total_fee_pct,
                    net_spread_pct=net_spread_pct,
                    flash_loan_usdc=0.0,
                    estimated_profit_usdc=0.0,
                    is_profitable=False,
                    timestamp=time.time(),
                    tier="DEPTH_REJECTED",
                    cost=cost,
                )
            # Use depth-discovered size (may be smaller than tier default)
            flash_loan_usdc = max_size
        else:
            # DEX config missing for at least one venue — cannot validate pool depth.
            # Reject rather than let an unchecked opp reach simulate_arb.
            logger.debug(
                "DEPTH_REJECTED | %s | buy=%s sell=%s | "
                "dex_cfg not found (pair_cfg=%s buy_cfg=%s sell_cfg=%s)",
                pair, buy_quote.venue, sell_quote.venue,
                pair_cfg is not None, buy_dex_cfg is not None, sell_dex_cfg is not None,
            )
            return ArbOpportunity(
                pair=pair,
                buy_venue=buy_quote.venue,
                sell_venue=sell_quote.venue,
                buy_price=buy_price,
                sell_price=sell_price,
                gross_spread_pct=gross_spread_pct,
                total_fee_pct=total_fee_pct,
                net_spread_pct=net_spread_pct,
                flash_loan_usdc=0.0,
                estimated_profit_usdc=0.0,
                is_profitable=False,
                timestamp=time.time(),
                tier="DEPTH_REJECTED",
                cost=cost,
            )
            cost = compute_cost_breakdown(
                gross_spread_pct=gross_spread_pct,
                lp_fee_buy_pct=buy_quote.fee_pct * 100.0,
                lp_fee_sell_pct=sell_quote.fee_pct * 100.0,
                price_impact_buy_pct=0.0,
                price_impact_sell_pct=0.0,
                flash_loan_usdc=flash_loan_usdc,
                gas_cost_usd=0.50,
            )

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
        cost=cost,
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
    w3: Optional[Web3] = None,
) -> List[ArbOpportunity]:
    """
    Scan all pairs and return ALL opportunities (sorted by net profit descending).
    Includes below-threshold opportunities (is_profitable=False) so caller can log them.
    DEPTH_REJECTED opps are included so the scan log shows them.

    prices format: {pair_name: [PriceQuote, ...]}  OR  {pair_name: (quote1, quote2)}
    w3: when provided, enables depth discovery via find_max_executable_size.
    """
    results: List[ArbOpportunity] = []

    for pair, quotes_raw in prices.items():
        quotes = list(quotes_raw)  # handle both tuple (legacy) and list (new)
        opp = _evaluate_pair_best(pair, quotes, min_spread_pct, max_flash_usdc, w3)
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
    Returns 0 if unknown. Fee-tier lists are ordered deepest-pool-first in config.
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


def _get_exec_fee(pair_name: str, venue_name: str) -> int:
    """
    Return the canonical execution fee for (pair, venue) from config.PAIR_EXEC_PARAMS.

    This is the fee used for the live contract call — guaranteed to match executor.py.
    For Aerodrome Slipstream, returns aero_tick (used as tickSpacing in the quoter).
    Falls back to 500 if the pair is not in PAIR_EXEC_PARAMS.
    """
    ep = config.PAIR_EXEC_PARAMS.get(pair_name, config._DEFAULT_PAIR_EXEC_PARAMS)
    n = venue_name.lower()
    if "pancake" in n:
        return ep["cake_fee"]
    if "aerodrome" in n or "slipstream" in n:
        return ep["aero_tick"]
    return ep["uni_fee"]  # Uniswap V3, BaseSwap, or unknown → uni_fee


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

    # Convert USD flash loan amount to borrow-token units.
    # For WETH pairs (borrow_dec=18) we must divide by ETH price first;
    # otherwise 17000 * 1e18 = 17,000 WETH raw (~$35M) instead of ~8.5 WETH.
    is_weth_borrow = borrow_token.lower() == config.WETH_ADDRESS.lower()
    if is_weth_borrow:
        eth_price_sim = _estimate_eth_price(w3)
        if eth_price_sim <= 0:
            eth_price_sim = 2000.0  # defensive fallback
        borrow_amount = usdc_in / eth_price_sim   # USD → WETH
    else:
        eth_price_sim = 0.0
        borrow_amount = usdc_in                    # USD → USDC (1:1)

    borrow_raw = int(borrow_amount * (10 ** borrow_dec))

    # Buy leg simulation
    try:
        if buy_quoter:
            fee = _get_exec_fee(opp.pair, opp.buy_venue)
            if fee == 0:
                fee = 500  # fallback
            q = w3.eth.contract(
                address=Web3.to_checksum_address(buy_quoter),
                abi=_QUOTER_V2_ABI,
            )
            result = q.functions.quoteExactInputSingle({
                "tokenIn":           Web3.to_checksum_address(borrow_token),
                "tokenOut":          Web3.to_checksum_address(intermediate),
                "amountIn":          borrow_raw,
                "fee":               fee,
                "sqrtPriceLimitX96": 0,
            }).call()
            token_amount = result[0] / (10 ** inter_dec)
        else:
            # No quoter (Aerodrome Slipstream / vAMM) — use scanner buy price
            token_amount = borrow_amount / opp.buy_price if opp.buy_price > 0 else 0.0
    except Exception as e:
        logger.debug("sim buy leg failed pair=%s dex=%s: %s", opp.pair, opp.buy_venue, e)
        token_amount = borrow_amount / opp.buy_price if opp.buy_price > 0 else 0.0

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
            fee = _get_exec_fee(opp.pair, opp.sell_venue)
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
    # Expected token from buy at scanner price — use borrow_amount (not usdc_in)
    # so WETH pairs get WETH/buy_price instead of USD/buy_price.
    expected_token = borrow_amount / opp.buy_price if opp.buy_price > 0 else 1.0
    buy_slippage = abs(token_amount - expected_token) / expected_token if expected_token > 0 else 0

    # For P&L, convert sell proceeds back to USD.
    usdc_out_usd = usdc_out * eth_price_sim if is_weth_borrow else usdc_out

    if buy_slippage > 0.02:
        return SimResult(
            buy_dex=opp.buy_venue, sell_dex=opp.sell_venue,
            token_amount=token_amount, usdc_in=usdc_in, usdc_out=usdc_out_usd,
            gross_profit_usd=usdc_out_usd - usdc_in, gas_cost_usd=gas_cost_usd,
            net_profit_usd=usdc_out_usd - usdc_in - gas_cost_usd,
            flash_provider=flash_provider, is_executable=False,
            rejection_reason=f"buy_slippage_too_high:{buy_slippage*100:.2f}%",
        )

    expected_borrow_out = token_amount * opp.sell_price  # in borrow-token units
    sell_slippage = abs(usdc_out - expected_borrow_out) / expected_borrow_out if expected_borrow_out > 0 else 0
    if sell_slippage > 0.02:
        return SimResult(
            buy_dex=opp.buy_venue, sell_dex=opp.sell_venue,
            token_amount=token_amount, usdc_in=usdc_in, usdc_out=usdc_out_usd,
            gross_profit_usd=usdc_out_usd - usdc_in, gas_cost_usd=gas_cost_usd,
            net_profit_usd=usdc_out_usd - usdc_in - gas_cost_usd,
            flash_provider=flash_provider, is_executable=False,
            rejection_reason=f"sell_slippage_too_high:{sell_slippage*100:.2f}%",
        )

    gross_profit = usdc_out_usd - usdc_in
    net_profit   = gross_profit - gas_cost_usd
    is_exec      = net_profit >= config.MIN_NET_PROFIT_USD

    return SimResult(
        buy_dex=opp.buy_venue,
        sell_dex=opp.sell_venue,
        token_amount=token_amount,
        usdc_in=usdc_in,
        usdc_out=usdc_out_usd,
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
