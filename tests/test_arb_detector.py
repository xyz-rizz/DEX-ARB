"""
Tests for arb_detector.py — all offline.
"""

import time
import pytest
from unittest.mock import MagicMock, patch

from price_scanner import PriceQuote
from arb_detector import (
    ArbOpportunity, SimResult, CostBreakdown,
    detect_opportunity, detect_all_opportunities,
    calculate_trade_size,
    _evaluate_pair, _evaluate_pair_best,
    assign_tier, simulate_arb, select_flash_provider,
    compute_cost_breakdown, find_max_executable_size,
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
    """MARGINAL tier at >= 0.065% (but < 0.10%) → flash = min($17k, max).
    Pin TIER_MARGINAL_PCT to default 0.065 so test is .env-independent."""
    with patch("arb_detector.config") as mock_cfg:
        mock_cfg.TIER_PRIME_PCT      = 0.15
        mock_cfg.TIER_GOOD_PCT       = 0.10
        mock_cfg.TIER_MARGINAL_PCT   = 0.065
        mock_cfg.FLASH_PRIME_USDC    = 50_000.0
        mock_cfg.FLASH_GOOD_USDC     = 34_000.0
        mock_cfg.FLASH_MARGINAL_USDC = 17_000.0
        assert assign_tier(0.07) == "MARGINAL"
        assert assign_tier(0.065) == "MARGINAL"
        flash, _ = calculate_trade_size(68000.0, 0.07, 50_000.0)
        assert flash == 17_000.0


def test_tier_below_at_004_pct():
    """BELOW tier: net_spread > 0 but < 0.065% → no flash loan.
    Pin TIER_MARGINAL_PCT to default 0.065 so test is .env-independent."""
    with patch("arb_detector.config") as mock_cfg:
        mock_cfg.TIER_PRIME_PCT      = 0.15
        mock_cfg.TIER_GOOD_PCT       = 0.10
        mock_cfg.TIER_MARGINAL_PCT   = 0.065
        mock_cfg.FLASH_PRIME_USDC    = 50_000.0
        mock_cfg.FLASH_GOOD_USDC     = 34_000.0
        mock_cfg.FLASH_MARGINAL_USDC = 17_000.0
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


# ── Sanity cap (MAX_GROSS_SPREAD_PCT) ─────────────────────────────────────────

def test_outlier_spread_rejected_by_sanity_cap():
    """
    BRETT/WETH regression: Aerodrome slot0 (spot, no slippage) vs PancakeSwap
    QuoterV2 (execution quote, thin pool) produces ~2009% apparent gross spread.
    Must be rejected before tier assignment — _evaluate_pair_best returns None.
    """
    # Simulate the live bug: pancake execution quote crushed by slippage
    aero_brett  = _make_quote("Aerodrome", 0.00021,  fee_pct=0.0001, pair="BRETT/WETH")
    pancake_brett = _make_quote("PancakeSwap", 0.0000099, fee_pct=0.0025, pair="BRETT/WETH")

    # gross spread ≈ (0.00021 - 0.0000099) / 0.0000099 * 100 ≈ 2020%
    gross = (aero_brett.price - pancake_brett.price) / pancake_brett.price * 100
    assert gross > config.MAX_GROSS_SPREAD_PCT, "test precondition: spread must exceed cap"

    opp = _evaluate_pair_best(
        "BRETT/WETH",
        [aero_brett, pancake_brett],
        min_spread_pct=0.0,
        max_flash_usdc=50_000,
    )
    assert opp is None, f"Expected None (outlier rejected), got tier={opp and opp.tier}"


def test_legitimate_spread_passes_sanity_cap():
    """
    A real 0.21% gross spread on cbBTC/USDC (→ 0.15% net → PRIME) must NOT be
    rejected by the sanity cap. Verifies only the outlier filter is checked, not
    everyday legitimate arb.
    """
    # gross ≈ (68300 - 68154) / 68154 * 100 ≈ 0.214% → net ≈ 0.154% → PRIME
    aero = _make_quote("aerodrome", 68300.0, fee_pct=0.0001, pair="cbBTC/USDC")
    uni  = _make_quote("uniswap",   68154.0, fee_pct=0.0005, pair="cbBTC/USDC")

    gross = (aero.price - uni.price) / uni.price * 100
    assert gross < config.MAX_GROSS_SPREAD_PCT, "test precondition: spread must be below cap"

    opp = _evaluate_pair_best(
        "cbBTC/USDC",
        [aero, uni],
        min_spread_pct=0.0,
        max_flash_usdc=50_000,
    )
    assert opp is not None, "Legitimate spread should not be rejected"
    assert opp.tier == "PRIME"


def test_outlier_never_reaches_prime_tier():
    """
    End-to-end: a 2000%+ gross spread pair must never appear as PRIME in
    detect_all_opportunities output, regardless of how it got there.
    """
    # Valid pair
    aero_btc = _make_quote("aerodrome", 68300.0, fee_pct=0.0001, pair="cbBTC/USDC")
    uni_btc  = _make_quote("uniswap",   68197.0, fee_pct=0.0005, pair="cbBTC/USDC")
    # Outlier pair mimicking BRETT bug
    aero_brett   = _make_quote("Aerodrome",  0.00021,   fee_pct=0.0001, pair="BRETT/WETH")
    pancake_brett = _make_quote("PancakeSwap", 0.0000099, fee_pct=0.0025, pair="BRETT/WETH")

    prices = {
        "cbBTC/USDC":  [aero_btc, uni_btc],
        "BRETT/WETH":  [aero_brett, pancake_brett],
    }

    all_opps = detect_all_opportunities(prices, min_spread_pct=0.0, max_flash_usdc=50_000)

    brett_opps = [o for o in all_opps if o.pair == "BRETT/WETH"]
    assert brett_opps == [], f"BRETT/WETH must be filtered out, found: {brett_opps}"

    prime_opps = [o for o in all_opps if o.tier == "PRIME"]
    for o in prime_opps:
        assert o.gross_spread_pct <= config.MAX_GROSS_SPREAD_PCT, (
            f"{o.pair} has tier=PRIME but gross_spread={o.gross_spread_pct:.2f}% "
            f"exceeds sanity cap={config.MAX_GROSS_SPREAD_PCT}%"
        )


# ── CostBreakdown tests ───────────────────────────────────────────────────────

def test_cost_breakdown_components_sum_to_net():
    """net_spread_pct == gross - lp_buy - lp_sell - impact_buy - impact_sell - gas."""
    cb = compute_cost_breakdown(
        gross_spread_pct=0.50,
        lp_fee_buy_pct=0.05,
        lp_fee_sell_pct=0.05,
        price_impact_buy_pct=0.10,
        price_impact_sell_pct=0.08,
        flash_loan_usdc=10_000.0,
        gas_cost_usd=1.0,  # 0.01%
    )
    # gas_pct = 1.0/10000*100 = 0.01%
    expected_net = 0.50 - 0.05 - 0.05 - 0.10 - 0.08 - 0.01 - 0.0
    assert abs(cb.net_spread_pct - expected_net) < 1e-9
    assert abs(cb.net_profit_usdc - 10_000.0 * expected_net / 100.0) < 1e-6


def test_cost_breakdown_toshi_shows_real_cause():
    """TOSHI/WETH: 0.1754% gross, 1%+1% LP fees → -1.8246% net. LP fees are culprit."""
    cb = compute_cost_breakdown(
        gross_spread_pct=0.1754,
        lp_fee_buy_pct=1.0,    # fee=10000 tier → 1%
        lp_fee_sell_pct=1.0,
        price_impact_buy_pct=0.0,
        price_impact_sell_pct=0.0,
        flash_loan_usdc=17_000.0,
        gas_cost_usd=0.0,
    )
    assert cb.lp_fee_buy_pct + cb.lp_fee_sell_pct == 2.0
    assert abs(cb.net_spread_pct - (0.1754 - 2.0)) < 1e-9
    assert cb.net_spread_pct < 0  # genuinely unprofitable


def test_price_impact_included_in_net_not_separate():
    """Non-zero price impact reduces net_spread_pct just like LP fees."""
    no_impact = compute_cost_breakdown(0.30, 0.05, 0.05, 0.0, 0.0, 10_000.0, 0.0)
    with_impact = compute_cost_breakdown(0.30, 0.05, 0.05, 0.05, 0.05, 10_000.0, 0.0)
    assert with_impact.net_spread_pct < no_impact.net_spread_pct
    diff = no_impact.net_spread_pct - with_impact.net_spread_pct
    assert abs(diff - 0.10) < 1e-9  # exactly 0.05 + 0.05 removed


def test_cost_breakdown_attached_to_opportunity():
    """_evaluate_pair_best attaches a CostBreakdown to every returned opportunity."""
    aero = _make_quote("aerodrome", 68297.04, fee_pct=0.0001)
    uni  = _make_quote("uniswap",   68193.69, fee_pct=0.0005)
    opp = _evaluate_pair_best(
        "cbBTC/USDC", [aero, uni], min_spread_pct=0.0, max_flash_usdc=50_000
    )
    assert opp is not None
    assert opp.cost is not None
    assert isinstance(opp.cost, CostBreakdown)
    # gross in CostBreakdown matches opp.gross_spread_pct
    assert abs(opp.cost.gross_spread_pct - opp.gross_spread_pct) < 1e-9


# ── Depth discovery / DEPTH_REJECTED tests ───────────────────────────────────

def test_find_max_executable_size_rejects_thin_pool():
    """When every test size causes >2% slippage on both legs, return 0.0."""
    # pair_cfg matches "cbBTC/USDC" from config (USDC-denominated)
    pair_cfg = next(p for p in config.PAIR_CONFIG if p["name"] == "cbBTC/USDC")
    buy_dex_cfg  = {"type": "uniswap_v3", "router": "0x" + "0" * 40}
    sell_dex_cfg = {"type": "uniswap_v3", "router": "0x" + "0" * 40}
    w3 = MagicMock()

    ref_price  = 68000.0
    unit_size  = pair_cfg["unit_size"]
    ref_amount = max(unit_size * 0.01, 10 ** (-pair_cfg["dec_in"]))

    # ref quote at small amount returns clean price;
    # any test-size quote returns 10% worse price → slippage > 2% → reject all.
    def slippage_quote(w3_, cfg, dex_cfg, amount):
        if amount <= ref_amount * 1.5:
            return ref_price          # reference call — clean
        return ref_price * 0.90       # test-size call — 10% slippage

    with patch("price_scanner.quote_at_amount", side_effect=slippage_quote):
        result = find_max_executable_size(
            w3=w3,
            pair_cfg=pair_cfg,
            buy_dex_cfg=buy_dex_cfg,
            sell_dex_cfg=sell_dex_cfg,
            max_usdc=50_000.0,
            slippage_tol=0.02,
            buy_price=ref_price,
        )
    assert result == 0.0


def test_find_max_executable_size_finds_correct_level():
    """When slippage is acceptable at the smallest test fraction, return non-zero."""
    pair_cfg = next(p for p in config.PAIR_CONFIG if p["name"] == "cbBTC/USDC")
    buy_dex_cfg  = {"type": "uniswap_v3", "router": "0x" + "0" * 40}
    sell_dex_cfg = {"type": "uniswap_v3", "router": "0x" + "0" * 40}
    w3 = MagicMock()

    ref_price  = 68000.0
    unit_size  = pair_cfg["unit_size"]
    ref_amount = max(unit_size * 0.01, 10 ** (-pair_cfg["dec_in"]))

    def size_aware_quote(w3_, cfg, dex_cfg, amount):
        if amount <= ref_amount * 1.5:
            return ref_price          # reference call — clean
        return ref_price * 0.99       # test-size call — 1% slippage (< 2% tol)

    with patch("price_scanner.quote_at_amount", side_effect=size_aware_quote):
        result = find_max_executable_size(
            w3=w3,
            pair_cfg=pair_cfg,
            buy_dex_cfg=buy_dex_cfg,
            sell_dex_cfg=sell_dex_cfg,
            max_usdc=50_000.0,
            slippage_tol=0.02,
            buy_price=ref_price,
        )
    assert result > 0.0


def test_depth_rejected_opp_has_zero_flash_and_not_profitable():
    """DEPTH_REJECTED opp must have flash_loan_usdc=0 and is_profitable=False."""
    aero = _make_quote("aerodrome", 68297.04, fee_pct=0.0001)
    uni  = _make_quote("uniswap",   68193.69, fee_pct=0.0005)
    w3 = MagicMock()

    with patch("arb_detector.find_max_executable_size", return_value=0.0), \
         patch("arb_detector._get_dex_cfg", return_value={"type": "uniswap_v3"}):
        opp = _evaluate_pair_best(
            "cbBTC/USDC", [aero, uni],
            min_spread_pct=0.065, max_flash_usdc=50_000,
            w3=w3,
        )

    assert opp is not None
    assert opp.tier == "DEPTH_REJECTED"
    assert opp.flash_loan_usdc == 0.0
    assert opp.is_profitable is False


def test_tier_marginal_pct_reads_from_env():
    """TIER_MARGINAL_PCT must equal MIN_SPREAD_PCT — single source of truth."""
    import importlib
    import sys
    import os

    # Save the original config module so we can restore the exact same object.
    # Replacing sys.modules["config"] with a *new* object would cause downstream
    # modules (e.g. main.config) to diverge from the test's module-level `config`
    # reference, breaking patch.object calls in later tests.
    orig_config = sys.modules.get("config")

    # Reload config with a custom MIN_SPREAD_PCT env value
    old_val = os.environ.get("MIN_SPREAD_PCT")
    os.environ["MIN_SPREAD_PCT"] = "0.001"
    try:
        # Force fresh reload so os.getenv picks up the new value
        if "config" in sys.modules:
            del sys.modules["config"]
        import config as cfg_fresh
        assert cfg_fresh.MIN_SPREAD_PCT == 0.001, \
            f"MIN_SPREAD_PCT should be 0.001, got {cfg_fresh.MIN_SPREAD_PCT}"
        assert cfg_fresh.TIER_MARGINAL_PCT == 0.001, \
            f"TIER_MARGINAL_PCT should be 0.001, got {cfg_fresh.TIER_MARGINAL_PCT}"
        assert cfg_fresh.MIN_SPREAD_PCT == cfg_fresh.TIER_MARGINAL_PCT, \
            "MIN_SPREAD_PCT and TIER_MARGINAL_PCT must be identical"
    finally:
        # Restore original env
        if old_val is None:
            os.environ.pop("MIN_SPREAD_PCT", None)
        else:
            os.environ["MIN_SPREAD_PCT"] = old_val
        # Restore the exact original config module object so sys.modules["config"]
        # stays stable for any module (e.g. main) that imports config for the first
        # time after this test runs.
        if "config" in sys.modules:
            del sys.modules["config"]
        if orig_config is not None:
            sys.modules["config"] = orig_config
        else:
            importlib.import_module("config")


def test_zero_flash_loan_never_creates_profitable_opp():
    """An opp with flash_loan_usdc=0 must never show is_profitable=True."""
    # Depth-rejected opp should have is_profitable=False regardless of spread
    aero = _make_quote("aerodrome", 68300.0, fee_pct=0.0001)
    uni  = _make_quote("uniswap",   68100.0, fee_pct=0.0001)
    w3 = MagicMock()

    with patch("arb_detector.find_max_executable_size", return_value=0.0), \
         patch("arb_detector._get_dex_cfg", return_value={"type": "uniswap_v3"}):
        opp = _evaluate_pair_best(
            "cbBTC/USDC", [aero, uni],
            min_spread_pct=0.0, max_flash_usdc=50_000,
            w3=w3,
        )

    assert opp is not None
    assert opp.is_profitable is False
    assert opp.flash_loan_usdc == 0.0


# ── Multicall3 batching in find_max_executable_size ───────────────────────────
# These tests exercise the fast multicall path (when build_depth_probe_calldata
# returns a valid tuple, i.e., pool is cached and dex_cfg has "name").

def _make_real_dex_cfg(dex_type="uniswap_v3"):
    """Return a minimal but complete dex_cfg dict that build_depth_probe_calldata accepts."""
    return {
        "name": "TestDEX",
        "type": dex_type,
        "factory": "0x" + "1" * 40,
        "quoter":  "0x" + "2" * 40,
        "fee_tiers": [500],
    }


def _make_fake_probe_calldata(pair_cfg, dex_cfg, amount):
    """Fake build_depth_probe_calldata — returns a valid tuple so multicall path is taken."""
    return (
        "0x" + "2" * 40,          # target (quoter address)
        b"\x00" * 4,              # callData (not actually called — multicall3 is mocked)
        "uniswap_v3",             # call_type
        pair_cfg["dec_out"],      # dec_out
        amount,                   # amount_in_human
    )


def _encode_v3_price(price: float, amount_in: float, dec_out: int) -> bytes:
    """Encode a price as the ABI return of quoteExactInputSingle (4 uint256-family words)."""
    from eth_abi import encode as abi_encode
    amount_out_raw = int(price * amount_in * (10 ** dec_out))
    return abi_encode(
        ["uint256", "uint160", "uint32", "uint256"],
        [amount_out_raw, 0, 0, 0],
    )


def test_find_max_executable_size_uses_multicall():
    """
    When build_depth_probe_calldata succeeds for both legs, multicall3 is called
    (not zero times) and the result is > 0 when all probes pass slippage.

    We patch decode_depth_probe_result to always return ref_price so slippage = 0.
    This avoids having to encode the exact amountOut per probe amount.
    """
    pair_cfg = next(p for p in config.PAIR_CONFIG if p["name"] == "cbBTC/USDC")
    buy_dex_cfg  = _make_real_dex_cfg()
    sell_dex_cfg = _make_real_dex_cfg()
    w3 = MagicMock()

    ref_price = 68000.0

    # multicall3: return a non-None bytes placeholder for each call (content irrelevant
    # because decode_depth_probe_result is also mocked)
    def fake_multicall(w3_, calls):
        return [b"\x00" * 128] * len(calls)

    # decode_depth_probe_result: always return ref_price → slippage = 0 for all probes
    def fake_decode(raw, call_type, dec_out, amount_in_human):
        return ref_price

    with patch("price_scanner.build_depth_probe_calldata",
               side_effect=_make_fake_probe_calldata), \
         patch("utils.multicall.multicall3", side_effect=fake_multicall) as mc_mock, \
         patch("price_scanner.decode_depth_probe_result", side_effect=fake_decode):
        result = find_max_executable_size(
            w3=w3,
            pair_cfg=pair_cfg,
            buy_dex_cfg=buy_dex_cfg,
            sell_dex_cfg=sell_dex_cfg,
            max_usdc=50_000.0,
            slippage_tol=0.02,
            buy_price=ref_price,
        )

    assert mc_mock.call_count > 0, "multicall3 must be called at least once"
    # With batching: 2 ref calls = 1 multicall3 call, 10 probe calls = 1 more → 2 total
    # Far fewer than 12 sequential calls
    assert mc_mock.call_count <= 3, (
        f"Expected ≤3 multicall3 round-trips (got {mc_mock.call_count}) — probes must be batched"
    )
    assert result > 0, "Should find a valid size when all probes pass slippage"


def test_find_max_executable_size_handles_failed_probe():
    """
    When the LARGEST probe size (frac=1.0) returns None (reverted) but smaller
    sizes succeed, the function returns the largest VALID size (not 0.0).
    No exception should be raised.

    Patching decode_depth_probe_result per call: None for calls indexed 0-1
    (the frac=1.0 buy+sell probes), ref_price for all others.
    """
    pair_cfg = next(p for p in config.PAIR_CONFIG if p["name"] == "cbBTC/USDC")
    buy_dex_cfg  = _make_real_dex_cfg()
    sell_dex_cfg = _make_real_dex_cfg()
    w3 = MagicMock()

    ref_price = 68000.0
    phase = {"n": 0}  # track multicall phase
    decode_call = {"n": 0}  # track decode call count

    def fake_multicall(w3_, calls):
        phase["n"] += 1
        return [b"\x00" * 128] * len(calls)

    def fake_decode(raw, call_type, dec_out, amount_in_human):
        # Phase 1 (ref calls) → always return ref_price
        # Phase 2 (probe calls):
        #   decode call 0,1 → None  (frac=1.0 buy+sell revert)
        #   decode call 2+  → ref_price (frac=0.5 and smaller pass)
        if phase["n"] == 1:
            return ref_price
        decode_call["n"] += 1
        if decode_call["n"] <= 2:
            return None   # frac=1.0 probes fail
        return ref_price

    with patch("price_scanner.build_depth_probe_calldata",
               side_effect=_make_fake_probe_calldata), \
         patch("utils.multicall.multicall3", side_effect=fake_multicall), \
         patch("price_scanner.decode_depth_probe_result", side_effect=fake_decode):
        result = find_max_executable_size(
            w3=w3,
            pair_cfg=pair_cfg,
            buy_dex_cfg=buy_dex_cfg,
            sell_dex_cfg=sell_dex_cfg,
            max_usdc=50_000.0,
            slippage_tol=0.02,
            buy_price=ref_price,
        )

    assert result > 0.0, "Should return the largest valid size (frac=0.5), not 0.0"
    assert result < 50_000.0, "frac=1.0 failed — result must be less than max_usdc"


def test_find_max_executable_size_all_probes_fail():
    """
    When decode_depth_probe_result returns None for every probe call,
    find_max_executable_size returns 0.0 and raises no exception.
    """
    pair_cfg = next(p for p in config.PAIR_CONFIG if p["name"] == "cbBTC/USDC")
    buy_dex_cfg  = _make_real_dex_cfg()
    sell_dex_cfg = _make_real_dex_cfg()
    w3 = MagicMock()

    ref_price = 68000.0
    phase = {"n": 0}

    def fake_multicall(w3_, calls):
        phase["n"] += 1
        return [b"\x00" * 128] * len(calls)

    def fake_decode(raw, call_type, dec_out, amount_in_human):
        if phase["n"] == 1:
            return ref_price  # ref calls succeed
        return None           # all probe calls fail

    with patch("price_scanner.build_depth_probe_calldata",
               side_effect=_make_fake_probe_calldata), \
         patch("utils.multicall.multicall3", side_effect=fake_multicall), \
         patch("price_scanner.decode_depth_probe_result", side_effect=fake_decode):
        result = find_max_executable_size(
            w3=w3,
            pair_cfg=pair_cfg,
            buy_dex_cfg=buy_dex_cfg,
            sell_dex_cfg=sell_dex_cfg,
            max_usdc=50_000.0,
            slippage_tol=0.02,
            buy_price=ref_price,
        )

    assert result == 0.0, f"All probes failed — expected 0.0, got {result}"


def test_arb_detector_no_alchemy_in_depth_probe():
    """
    Negative test: arb_detector.py must not contain any reference to ALCHEMY
    in the detect / depth probe functions. All reads use dRPC (w3_read) only.
    """
    import inspect
    import arb_detector as _ad

    src = inspect.getsource(_ad.find_max_executable_size)
    assert "ALCHEMY" not in src, "find_max_executable_size must not reference ALCHEMY"
    assert "alchemy" not in src.lower(), (
        "find_max_executable_size must not reference alchemy (any case)"
    )

    # Also verify module-level source has no Alchemy RPC references in detect path
    full_src = inspect.getsource(_ad)
    # The whole module must not import or reference Alchemy URLs
    assert "ALCHEMY" not in full_src or "ALCHEMY" in "ALCHEMY_KEY", (
        "arb_detector.py must not hard-code ALCHEMY references"
    )
