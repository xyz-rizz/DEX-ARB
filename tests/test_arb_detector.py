"""
Tests for arb_detector.py — all offline.
"""

import time
import pytest
from unittest.mock import MagicMock, patch

from price_scanner import PriceQuote
from arb_detector import (
    ArbOpportunity, SimResult,
    detect_opportunity, detect_all_opportunities,
    calculate_trade_size,
    _evaluate_pair, _evaluate_pair_best,
    assign_tier, simulate_arb, select_flash_provider,
)
import config


def _make_quote(venue, price, fee_pct=0.0001, pair="cbBTC/USDC"):
    return PriceQuote(
        venue=venue, pair=pair, price=price,
        fee_pct=fee_pct, block=1, timestamp=time.time(),
    )


# ── detect_opportunity ────────────────────────────────────────────────────────

def test_profitable_opportunity_detected():
    """aero=68297, uni=68193 → net=0.0916% → profitable (MARGINAL tier)"""
    aero = _make_quote("aerodrome", 68297.04, fee_pct=0.0001)
    uni  = _make_quote("uniswap",   68193.69, fee_pct=0.0005)
    prices = {"cbBTC/USDC": (aero, uni)}

    opp = detect_opportunity(prices, min_spread_pct=0.065, max_flash_usdc=50000)

    assert opp is not None
    assert opp.is_profitable is True
    assert opp.net_spread_pct >= 0.065
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
    """When Aerodrome price > Uniswap, buy on Uniswap, sell on Aerodrome."""
    aero = _make_quote("aerodrome", 68297.04, fee_pct=0.0001)
    uni  = _make_quote("uniswap",   68193.69, fee_pct=0.0005)
    prices = {"cbBTC/USDC": (aero, uni)}

    opp = detect_opportunity(prices, min_spread_pct=0.0, max_flash_usdc=50000)

    assert opp is not None
    assert opp.buy_venue  == "uniswap"
    assert opp.sell_venue == "aerodrome"


def test_correct_buy_sell_assignment_uni_higher():
    """When Uniswap price > Aerodrome, buy on Aerodrome, sell on Uniswap."""
    aero = _make_quote("aerodrome", 68100.0, fee_pct=0.0001)
    uni  = _make_quote("uniswap",   68500.0, fee_pct=0.0005)
    prices = {"cbBTC/USDC": (aero, uni)}

    opp = detect_opportunity(prices, min_spread_pct=0.0, max_flash_usdc=50000)

    assert opp is not None
    assert opp.buy_venue  == "aerodrome"
    assert opp.sell_venue == "uniswap"


def test_trade_size_never_exceeds_max_flash_loan():
    """Flash loan amount must never exceed max_usdc."""
    flash, _ = calculate_trade_size(
        buy_price=68000.0, net_spread_pct=0.15, max_usdc=50000.0
    )
    assert flash <= 50000.0


def test_trade_size_conservative_cap():
    """GOOD tier (0.10% net spread) is capped at $34k."""
    flash, _ = calculate_trade_size(
        buy_price=68000.0, net_spread_pct=0.10, max_usdc=100_000.0
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
    """Equal prices → gross=0 → net < 0 → not profitable."""
    aero = _make_quote("aerodrome", 68000.0, fee_pct=0.0001)
    uni  = _make_quote("uniswap",   68000.0, fee_pct=0.0005)
    prices = {"cbBTC/USDC": (aero, uni)}

    opp = detect_opportunity(prices, min_spread_pct=0.08, max_flash_usdc=50000)

    assert opp is not None
    assert opp.is_profitable is False
    assert opp.gross_spread_pct == 0.0


def test_fee_correctly_subtracted():
    """total_fee_pct = (aero_fee + uni_fee) * 100."""
    aero = _make_quote("aerodrome", 68297.04, fee_pct=0.0001)
    uni  = _make_quote("uniswap",   68193.69, fee_pct=0.0005)
    prices = {"cbBTC/USDC": (aero, uni)}

    opp = detect_opportunity(prices, min_spread_pct=0.0, max_flash_usdc=50000)

    assert opp is not None
    expected_fee = (0.0001 + 0.0005) * 100
    assert abs(opp.total_fee_pct - expected_fee) < 0.0001


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

    assert opp is not None
    assert opp.pair == "cbBTC/USDC"  # far higher absolute profit


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


# ── New tier tests ────────────────────────────────────────────────────────────

def test_profit_calculation_not_zero():
    """estimated_profit_usdc must be > 0 for any profitable opportunity."""
    aero = _make_quote("aerodrome", 68297.04, fee_pct=0.0001)
    uni  = _make_quote("uniswap",   68193.69, fee_pct=0.0005)
    prices = {"cbBTC/USDC": (aero, uni)}

    opp = detect_opportunity(prices, min_spread_pct=0.065, max_flash_usdc=50000)

    assert opp is not None
    assert opp.estimated_profit_usdc > 0, "profit must not be $0.00"


def test_tier_prime_at_015_pct():
    """PRIME tier at >= 0.15% net spread → flash = min($50k, max)."""
    assert assign_tier(0.15) == "PRIME"
    assert assign_tier(0.20) == "PRIME"

    flash, _ = calculate_trade_size(68000.0, 0.15, 50_000.0)
    assert flash == 50_000.0


def test_tier_good_at_010_pct():
    """GOOD tier at >= 0.10% (but < 0.15%) → flash = min($34k, max)."""
    assert assign_tier(0.10) == "GOOD"
    assert assign_tier(0.14) == "GOOD"

    flash, _ = calculate_trade_size(68000.0, 0.10, 50_000.0)
    assert flash == 34_000.0


def test_tier_marginal_at_007_pct():
    """MARGINAL tier at >= 0.065% (but < 0.10%) → flash = min($17k, max)."""
    assert assign_tier(0.07) == "MARGINAL"
    assert assign_tier(0.065) == "MARGINAL"

    flash, _ = calculate_trade_size(68000.0, 0.07, 50_000.0)
    assert flash == 17_000.0


def test_tier_below_at_004_pct():
    """BELOW tier: net_spread > 0 but < 0.065% → no flash loan."""
    assert assign_tier(0.04) == "BELOW"
    assert assign_tier(0.001) == "BELOW"

    flash, _ = calculate_trade_size(68000.0, 0.04, 50_000.0)
    assert flash == 0.0


def test_tier_no_arb_at_negative():
    """NO_ARB tier: net_spread <= 0 → no flash loan."""
    assert assign_tier(0.0) == "NO_ARB"
    assert assign_tier(-0.01) == "NO_ARB"


def test_opportunity_has_tier_field():
    """ArbOpportunity must have a tier field set."""
    aero = _make_quote("aerodrome", 68297.04, fee_pct=0.0001)
    uni  = _make_quote("uniswap",   68193.69, fee_pct=0.0005)
    prices = {"cbBTC/USDC": (aero, uni)}

    opp = detect_opportunity(prices, min_spread_pct=0.065, max_flash_usdc=50000)

    assert hasattr(opp, "tier")
    assert opp.tier in ("PRIME", "GOOD", "MARGINAL", "BELOW", "NO_ARB")


# ── Flash loan provider tests ─────────────────────────────────────────────────

def test_flash_loan_morpho_selected_first():
    """Morpho is selected first when it has enough USDC."""
    w3 = MagicMock()
    erc20 = MagicMock()
    w3.eth.contract.return_value = erc20
    # Morpho has 100k USDC
    erc20.functions.balanceOf.return_value.call.return_value = int(100_000 * 1e6)

    provider = select_flash_provider(w3, 50_000.0)
    assert provider == "Morpho"


def test_flash_loan_balancer_fallback():
    """Balancer selected when Morpho doesn't have enough."""
    w3 = MagicMock()

    call_count = {"n": 0}
    def side_effect(*args, **kwargs):
        # First call = Morpho (insufficient), second = Balancer (sufficient)
        call_count["n"] += 1
        m = MagicMock()
        if call_count["n"] == 1:
            m.return_value = int(100 * 1e6)  # 100 USDC — not enough
        else:
            m.return_value = int(1_000_000 * 1e6)  # 1M USDC — sufficient
        return m

    erc20 = MagicMock()
    w3.eth.contract.return_value = erc20

    balance_calls = [int(100 * 1e6), int(1_000_000 * 1e6)]
    erc20.functions.balanceOf.return_value.call.side_effect = balance_calls

    provider = select_flash_provider(w3, 50_000.0)
    assert provider == "Balancer"


def test_flash_loan_no_provider_when_both_insufficient():
    """Returns '' when neither Morpho nor Balancer has enough."""
    w3 = MagicMock()
    erc20 = MagicMock()
    w3.eth.contract.return_value = erc20
    erc20.functions.balanceOf.return_value.call.return_value = int(100 * 1e6)  # 100 USDC

    provider = select_flash_provider(w3, 50_000.0)
    assert provider == ""


# ── Simulation tests ──────────────────────────────────────────────────────────

def test_simulate_arb_rejects_on_gas():
    """simulate_arb rejects when net profit < MIN_NET_PROFIT_USD after gas."""
    w3 = MagicMock()
    opp = ArbOpportunity(
        pair="cbBTC/USDC",
        buy_venue="Uniswap V3",
        sell_venue="Aerodrome Slipstream",
        buy_price=68193.69,
        sell_price=68297.04,
        gross_spread_pct=0.1516,
        total_fee_pct=0.06,
        net_spread_pct=0.0916,
        flash_loan_usdc=17_000.0,
        estimated_profit_usdc=15.57,
        is_profitable=True,
        timestamp=time.time(),
        tier="MARGINAL",
    )

    # Simulate: gas cost is enormous → net profit negative
    with patch("arb_detector.select_flash_provider", return_value="Morpho"), \
         patch("arb_detector._get_dex_best_fee", return_value=500), \
         patch("arb_detector._estimate_eth_price", return_value=3500.0):
        erc20 = MagicMock()
        w3.eth.contract.return_value = erc20
        erc20.functions.balanceOf.return_value.call.return_value = int(100_000 * 1e6)
        # Return a realistic token amount so slippage check passes
        erc20.functions.quoteExactInputSingle.return_value.call.return_value = [
            int(0.249 * 1e8), 0, 0, 100_000
        ]
        w3.eth.gas_price = int(1e15)  # 1,000,000 gwei — unrealistically high

        sim = simulate_arb(w3, opp)

    # With 1000 gwei gas, cost = 400000 * 1e15 * 3500 / 1e18 = 1400 USD >> profit
    assert sim.is_executable is False
    assert "below_min_profit" in sim.rejection_reason or sim.gas_cost_usd > 100


def test_simulate_arb_rejects_on_slippage():
    """simulate_arb rejects when simulated token amount differs > 2% from expected."""
    w3 = MagicMock()
    opp = ArbOpportunity(
        pair="cbBTC/USDC",
        buy_venue="Uniswap V3",
        sell_venue="Aerodrome Slipstream",
        buy_price=68193.69,
        sell_price=68297.04,
        gross_spread_pct=0.1516,
        total_fee_pct=0.06,
        net_spread_pct=0.0916,
        flash_loan_usdc=17_000.0,
        estimated_profit_usdc=15.57,
        is_profitable=True,
        timestamp=time.time(),
        tier="MARGINAL",
    )

    # Return a buy quote that implies 5% slippage (far less token than expected)
    expected_token = 17000 / 68193.69  # ~0.249 cbBTC
    slipped_token = expected_token * 0.94  # 6% slippage

    mock_quoter = MagicMock()
    w3.eth.contract.return_value = mock_quoter

    # Buy leg: returns slipped amount (in raw 8-dec cbBTC)
    mock_quoter.functions.quoteExactInputSingle.return_value.call.return_value = [
        int(slipped_token * 1e8), 0, 0, 100000
    ]
    mock_quoter.functions.balanceOf.return_value.call.return_value = int(100_000 * 1e6)

    with patch("arb_detector.select_flash_provider", return_value="Morpho"), \
         patch("arb_detector._get_dex_best_fee", return_value=500), \
         patch("arb_detector._estimate_eth_price", return_value=3500.0):
        w3.eth.gas_price = int(1e9)  # 1 gwei — normal
        sim = simulate_arb(w3, opp)

    assert sim.is_executable is False
    assert "slippage" in sim.rejection_reason


def test_best_opportunity_is_highest_net_profit():
    """detect_all_opportunities sorts by estimated_profit_usdc descending."""
    # cbBTC: big spread → high profit
    aero_btc = _make_quote("aerodrome", 68300.0, fee_pct=0.0001, pair="cbBTC/USDC")
    uni_btc  = _make_quote("uniswap",   68100.0, fee_pct=0.0005, pair="cbBTC/USDC")
    # weETH: tiny spread → small profit
    aero_eth = _make_quote("aerodrome", 1.091,   fee_pct=0.0001, pair="weETH/WETH")
    uni_eth  = _make_quote("uniswap",   1.089,   fee_pct=0.0001, pair="weETH/WETH")

    prices = {
        "cbBTC/USDC": (aero_btc, uni_btc),
        "weETH/WETH": (aero_eth, uni_eth),
    }

    all_opps = detect_all_opportunities(prices, min_spread_pct=0.0, max_flash_usdc=50000)

    assert len(all_opps) >= 1
    # First opportunity must have highest profit
    for i in range(1, len(all_opps)):
        assert all_opps[0].estimated_profit_usdc >= all_opps[i].estimated_profit_usdc
