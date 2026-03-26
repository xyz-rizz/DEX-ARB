"""
Arbitrage opportunity detector for DEX Arb Bot.
Compares Aerodrome vs Uniswap prices and calculates net profit after fees.
Never imports from the morpho_scanner liquidation bot.
"""

import time
from dataclasses import dataclass
from typing import Optional

from price_scanner import PriceQuote
import config


@dataclass
class ArbOpportunity:
    pair: str
    buy_venue: str              # cheaper venue (buy here)
    sell_venue: str             # more expensive venue (sell here)
    buy_price: float            # price at which we buy (cheaper)
    sell_price: float           # price at which we sell (more expensive)
    gross_spread_pct: float     # (sell - buy) / buy * 100
    total_fee_pct: float        # sum of both venues' fees as percentage
    net_spread_pct: float       # gross_spread_pct - total_fee_pct
    flash_loan_usdc: float      # USDC to borrow for the trade
    estimated_profit_usdc: float
    is_profitable: bool
    timestamp: float

    def __repr__(self) -> str:
        tag = "PROFITABLE" if self.is_profitable else "BELOW_THRESHOLD"
        return (
            f"ArbOpportunity({self.pair} {tag} "
            f"buy={self.buy_venue}@{self.buy_price:.2f} "
            f"sell={self.sell_venue}@{self.sell_price:.2f} "
            f"net={self.net_spread_pct:.4f}% "
            f"profit=${self.estimated_profit_usdc:.2f})"
        )


def calculate_trade_size(
    buy_price: float,
    net_spread_pct: float,
    max_usdc: float,
) -> tuple:
    """
    Calculate conservative trade size in USDC and the corresponding asset amount.

    Strategy:
    - Start at min($34,000, max_usdc) to limit slippage on cbBTC pools
    - For weETH/WETH, use a smaller notional (1 ETH equivalent ≈ $3,500)
    - Do not exceed what would generate > 2% slippage on either pool

    Returns:
        (usdc_amount, asset_amount)
    """
    # Conservative start: $34k cap avoids significant slippage on cbBTC pools
    conservative_cap = min(34_000.0, max_usdc)
    usdc_amount = conservative_cap

    # If net spread is thin, scale down to reduce risk
    if net_spread_pct < 0.1:
        usdc_amount = min(usdc_amount, 20_000.0)

    # Asset amount (e.g. for cbBTC: usdc / price)
    asset_amount = usdc_amount / buy_price if buy_price > 0 else 0.0

    return usdc_amount, asset_amount


def detect_opportunity(
    prices: dict,
    min_spread_pct: float,
    max_flash_usdc: float,
) -> Optional[ArbOpportunity]:
    """
    Scan all pairs and return the best arbitrage opportunity, or None.

    Compares Aerodrome price vs Uniswap price for each pair.
    Buys on cheaper venue, sells on more expensive venue.
    Returns None if no pair exceeds min_spread_pct after fees.

    Args:
        prices: dict from get_all_prices() →
            {"cbBTC/USDC": (aero_quote, uni_quote), ...}
        min_spread_pct: minimum net spread in percent (e.g. 0.08 for 0.08%)
        max_flash_usdc: maximum USDC flash loan size

    Returns:
        Best ArbOpportunity or None
    """
    best: Optional[ArbOpportunity] = None

    for pair, (aero_quote, uni_quote) in prices.items():
        opp = _evaluate_pair(pair, aero_quote, uni_quote, min_spread_pct, max_flash_usdc)
        if opp is None:
            continue
        if best is None or opp.estimated_profit_usdc > best.estimated_profit_usdc:
            best = opp

    return best


def _evaluate_pair(
    pair: str,
    aero_quote: PriceQuote,
    uni_quote: PriceQuote,
    min_spread_pct: float,
    max_flash_usdc: float,
) -> Optional[ArbOpportunity]:
    """Evaluate a single pair for arbitrage and return an opportunity or None."""
    aero_price = aero_quote.price
    uni_price  = uni_quote.price

    if aero_price <= 0 or uni_price <= 0:
        return None

    # Determine buy/sell direction
    if aero_price > uni_price:
        # Aerodrome is more expensive → sell on Aerodrome, buy on Uniswap
        buy_venue  = "uniswap"
        sell_venue = "aerodrome"
        buy_price  = uni_price
        sell_price = aero_price
        buy_fee    = aero_quote.fee_pct   # Aerodrome fee for the sell leg
        sell_fee   = uni_quote.fee_pct    # Uniswap fee for the buy leg
    else:
        # Uniswap is more expensive → sell on Uniswap, buy on Aerodrome
        buy_venue  = "aerodrome"
        sell_venue = "uniswap"
        buy_price  = aero_price
        sell_price = uni_price
        buy_fee    = aero_quote.fee_pct
        sell_fee   = uni_quote.fee_pct

    gross_spread_pct = (sell_price - buy_price) / buy_price * 100.0
    total_fee_pct    = (buy_fee + sell_fee) * 100.0
    net_spread_pct   = gross_spread_pct - total_fee_pct

    is_profitable = net_spread_pct >= min_spread_pct

    # Calculate trade size and estimated profit
    flash_loan_usdc, _asset_amount = calculate_trade_size(
        buy_price=buy_price,
        net_spread_pct=net_spread_pct,
        max_usdc=max_flash_usdc,
    )

    # Estimated profit = flash_loan * (net_spread / 100)
    estimated_profit_usdc = flash_loan_usdc * (net_spread_pct / 100.0)

    return ArbOpportunity(
        pair=pair,
        buy_venue=buy_venue,
        sell_venue=sell_venue,
        buy_price=buy_price,
        sell_price=sell_price,
        gross_spread_pct=gross_spread_pct,
        total_fee_pct=total_fee_pct,
        net_spread_pct=net_spread_pct,
        flash_loan_usdc=flash_loan_usdc,
        estimated_profit_usdc=estimated_profit_usdc,
        is_profitable=is_profitable,
        timestamp=time.time(),
    )
