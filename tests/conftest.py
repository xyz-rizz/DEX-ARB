"""
Test fixtures for DEX Arbitrage Bot.
All tests run offline — no live RPC calls required except where explicitly noted.
"""

import time
import pytest
from unittest.mock import MagicMock, patch

from price_scanner import PriceQuote
from arb_detector import ArbOpportunity


# ── Price quote fixtures ───────────────────────────────────────────────────────

@pytest.fixture
def aero_cbbtc_quote():
    """Aerodrome cbBTC/USDC quote: ~$68,297 USDC per cbBTC."""
    return PriceQuote(
        venue="aerodrome",
        pair="cbBTC/USDC",
        price=68297.04,
        fee_pct=0.0001,
        block=12345678,
        timestamp=time.time(),
    )


@pytest.fixture
def uni_cbbtc_quote():
    """Uniswap cbBTC/USDC quote: ~$68,193 USDC per cbBTC."""
    return PriceQuote(
        venue="uniswap",
        pair="cbBTC/USDC",
        price=68193.69,
        fee_pct=0.0005,
        block=12345678,
        timestamp=time.time(),
    )


@pytest.fixture
def equal_prices_cbbtc():
    """Both venues at same price — no opportunity."""
    q1 = PriceQuote(venue="aerodrome", pair="cbBTC/USDC", price=68000.0,
                    fee_pct=0.0001, block=1, timestamp=time.time())
    q2 = PriceQuote(venue="uniswap",   pair="cbBTC/USDC", price=68000.0,
                    fee_pct=0.0005, block=1, timestamp=time.time())
    return q1, q2


@pytest.fixture
def profitable_opportunity():
    return ArbOpportunity(
        pair="cbBTC/USDC",
        buy_venue="uniswap",
        sell_venue="aerodrome",
        buy_price=68193.69,
        sell_price=68297.04,
        gross_spread_pct=0.1516,
        total_fee_pct=0.06,
        net_spread_pct=0.0916,
        flash_loan_usdc=34000.0,
        estimated_profit_usdc=31.14,
        is_profitable=True,
        timestamp=time.time(),
    )


@pytest.fixture
def below_threshold_opportunity():
    return ArbOpportunity(
        pair="cbBTC/USDC",
        buy_venue="uniswap",
        sell_venue="aerodrome",
        buy_price=68000.0,
        sell_price=68010.0,
        gross_spread_pct=0.0147,
        total_fee_pct=0.06,
        net_spread_pct=-0.0453,
        flash_loan_usdc=20000.0,
        estimated_profit_usdc=-9.06,
        is_profitable=False,
        timestamp=time.time(),
    )
