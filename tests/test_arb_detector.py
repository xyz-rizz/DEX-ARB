"""
Tests for arb_detector.py — all offline.
"""

import time
import pytest

from price_scanner import PriceQuote
from arb_detector import (
    ArbOpportunity,
    detect_opportunity,
    calculate_trade_size,
    _evaluate_pair,
)
import config


def _make_quote(venue, price, fee_pct=0.0001, pair="cbBTC/USDC"):
    return PriceQuote(
        venue=venue, pair=pair, price=price,
        fee_pct=fee_pct, block=1, timestamp=time.time(),
    )


# ── detect_opportunity ────────────────────────────────────────────────────────

def test_profitable_opportunity_detected():
    """
    aero=68297, uni=68193 → gross=0.1516%, fees=0.06%, net=0.0916% → profitable
    """
    aero = _make_quote("aerodrome", 68297.04, fee_pct=0.0001)
    uni  = _make_quote("uniswap",   68193.69, fee_pct=0.0005)
    prices = {"cbBTC/USDC": (aero, uni)}

    opp = detect_opportunity(prices, min_spread_pct=0.08, max_flash_usdc=50000)

    assert opp is not None
    assert opp.is_profitable is True
    assert opp.net_spread_pct >= 0.08
    assert opp.estimated_profit_usdc > 0


def test_below_threshold_not_profitable():
    """Spread < min_spread_pct → is_profitable=False."""
    aero = _make_quote("aerodrome", 68010.0, fee_pct=0.0001)
    uni  = _make_quote("uniswap",   68000.0, fee_pct=0.0005)
    prices = {"cbBTC/USDC": (aero, uni)}

    opp = detect_opportunity(prices, min_spread_pct=0.08, max_flash_usdc=50000)

    assert opp is not None
    assert opp.is_profitable is False


def test_correct_buy_sell_assignment_aero_higher():
    """When Aerodrome price > Uniswap, we buy on Uniswap and sell on Aerodrome."""
    aero = _make_quote("aerodrome", 68297.04, fee_pct=0.0001)
    uni  = _make_quote("uniswap",   68193.69, fee_pct=0.0005)
    prices = {"cbBTC/USDC": (aero, uni)}

    opp = detect_opportunity(prices, min_spread_pct=0.0, max_flash_usdc=50000)

    assert opp is not None
    assert opp.buy_venue  == "uniswap"
    assert opp.sell_venue == "aerodrome"


def test_correct_buy_sell_assignment_uni_higher():
    """When Uniswap price > Aerodrome, we buy on Aerodrome and sell on Uniswap."""
    aero = _make_quote("aerodrome", 68100.0, fee_pct=0.0001)
    uni  = _make_quote("uniswap",   68500.0, fee_pct=0.0005)
    prices = {"cbBTC/USDC": (aero, uni)}

    opp = detect_opportunity(prices, min_spread_pct=0.0, max_flash_usdc=50000)

    assert opp is not None
    assert opp.buy_venue  == "aerodrome"
    assert opp.sell_venue == "uniswap"


def test_trade_size_never_exceeds_max_flash_loan():
    """Flash loan amount must never exceed MAX_FLASH_LOAN_USDC."""
    flash, _ = calculate_trade_size(
        buy_price=68000.0,
        net_spread_pct=0.15,
        max_usdc=50000.0,
    )
    assert flash <= 50000.0


def test_trade_size_conservative_cap():
    """Trade size is capped at $34k for cbBTC pools (slippage protection)."""
    flash, _ = calculate_trade_size(
        buy_price=68000.0,
        net_spread_pct=0.15,
        max_usdc=100_000.0,  # allow large sizes
    )
    assert flash <= 34_000.0


def test_profit_calculation_correct():
    """estimated_profit_usdc = flash_loan * net_spread / 100."""
    aero = _make_quote("aerodrome", 68297.04, fee_pct=0.0001)
    uni  = _make_quote("uniswap",   68193.69, fee_pct=0.0005)
    prices = {"cbBTC/USDC": (aero, uni)}

    opp = detect_opportunity(prices, min_spread_pct=0.0, max_flash_usdc=50000)

    assert opp is not None
    expected = opp.flash_loan_usdc * opp.net_spread_pct / 100.0
    assert abs(opp.estimated_profit_usdc - expected) < 0.01


def test_no_opportunity_when_prices_equal():
    """Equal prices → net spread = 0 - fees → not profitable."""
    aero = _make_quote("aerodrome", 68000.0, fee_pct=0.0001)
    uni  = _make_quote("uniswap",   68000.0, fee_pct=0.0005)
    prices = {"cbBTC/USDC": (aero, uni)}

    opp = detect_opportunity(prices, min_spread_pct=0.08, max_flash_usdc=50000)

    assert opp is not None  # still returns, but not profitable
    assert opp.is_profitable is False
    assert opp.gross_spread_pct == 0.0


def test_fee_correctly_subtracted():
    """total_fee_pct = (aero_fee + uni_fee) * 100."""
    aero = _make_quote("aerodrome", 68297.04, fee_pct=0.0001)
    uni  = _make_quote("uniswap",   68193.69, fee_pct=0.0005)
    prices = {"cbBTC/USDC": (aero, uni)}

    opp = detect_opportunity(prices, min_spread_pct=0.0, max_flash_usdc=50000)

    assert opp is not None
    expected_fee_pct = (0.0001 + 0.0005) * 100
    assert abs(opp.total_fee_pct - expected_fee_pct) < 0.0001


def test_detect_returns_none_when_prices_empty():
    """Empty prices dict returns None."""
    opp = detect_opportunity({}, min_spread_pct=0.08, max_flash_usdc=50000)
    assert opp is None


def test_returns_best_opportunity_from_multiple_pairs():
    """detect_opportunity returns the most profitable pair."""
    aero_cbbtc = _make_quote("aerodrome", 68297.04, fee_pct=0.0001, pair="cbBTC/USDC")
    uni_cbbtc  = _make_quote("uniswap",   68193.69, fee_pct=0.0005, pair="cbBTC/USDC")
    aero_weeth = _make_quote("aerodrome", 1.090,    fee_pct=0.0001, pair="weETH/WETH")
    uni_weeth  = _make_quote("uniswap",   1.089,    fee_pct=0.0001, pair="weETH/WETH")

    prices = {
        "cbBTC/USDC": (aero_cbbtc, uni_cbbtc),
        "weETH/WETH": (aero_weeth, uni_weeth),
    }

    opp = detect_opportunity(prices, min_spread_pct=0.0, max_flash_usdc=50000)

    # cbBTC/USDC has far higher absolute profit
    assert opp is not None
    assert opp.pair == "cbBTC/USDC"


def test_gross_spread_math():
    """gross_spread_pct = (sell - buy) / buy * 100."""
    opp = _evaluate_pair(
        "cbBTC/USDC",
        _make_quote("aerodrome", 68297.04, fee_pct=0.0001),
        _make_quote("uniswap",   68193.69, fee_pct=0.0005),
        min_spread_pct=0.0,
        max_flash_usdc=50000,
    )
    expected_gross = (68297.04 - 68193.69) / 68193.69 * 100
    assert abs(opp.gross_spread_pct - expected_gross) < 0.0001
