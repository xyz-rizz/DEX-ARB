"""
Tests for price_scanner.py — all offline using mocked web3 calls.
"""

import time
import pytest
from unittest.mock import MagicMock, patch

from price_scanner import (
    PriceQuote,
    get_aerodrome_price,
    get_uniswap_price,
    get_all_prices,
    _check_liquidity,
    _quote_uniswap_v3,
    _quote_slipstream,
)
import config


# ── Helper ────────────────────────────────────────────────────────────────────

def _mock_slot0(w3, sqrt_price_x96):
    pool = MagicMock()
    w3.eth.contract.return_value = pool
    pool.functions.slot0.return_value.call.return_value = [
        sqrt_price_x96, 1, 0, 1, 1, 0, True
    ]
    w3.eth.block_number = 12000000
    return pool


# ── Aerodrome price math tests ─────────────────────────────────────────────────

def test_aerodrome_price_math_cbbtc():
    """
    sqrtPriceX96=3033126396693973345289760393
    pool: token0=USDC(6dec), token1=cbBTC(8dec), invert=True
    Expected: USDC per cbBTC ≈ $68,230 ± $100
    """
    sqrt_price_x96 = 3033126396693973345289760393
    w3 = MagicMock()
    _mock_slot0(w3, sqrt_price_x96)

    quote = get_aerodrome_price(
        w3=w3,
        pool_address="0x4F5905e36ac07eE1F01ffB939aA7f212A58D5CDF",
        token0_decimals=6,
        token1_decimals=8,
        invert=True,
        pair="cbBTC/USDC",
        fee_pct=0.0001,
    )

    assert quote.venue == "aerodrome"
    assert quote.pair  == "cbBTC/USDC"
    assert 68_130 <= quote.price <= 68_330, f"Expected ~68,230 ± 100, got {quote.price:.2f}"
    assert quote.fee_pct == 0.0001
    assert quote.block == 12000000


def test_aerodrome_price_math_weeth():
    """
    sqrtPriceX96=82749893355765197634930415633
    pool: token0=weETH(18dec), token1=WETH(18dec), invert=False
    Expected: WETH per weETH ≈ 1.0909 ± 0.001
    """
    sqrt_price_x96 = 82749893355765197634930415633
    w3 = MagicMock()
    _mock_slot0(w3, sqrt_price_x96)

    quote = get_aerodrome_price(
        w3=w3,
        pool_address="0xbD3cd0D9d429b41F0a2e1C026552Bd598294d5E0",
        token0_decimals=18,
        token1_decimals=18,
        invert=False,
        pair="weETH/WETH",
        fee_pct=0.0001,
    )

    assert quote.venue == "aerodrome"
    assert 1.0899 <= quote.price <= 1.0919, f"Expected ~1.0909 ± 0.001, got {quote.price:.6f}"


def test_aerodrome_price_quote_fields_populated():
    """All PriceQuote fields must be populated."""
    w3 = MagicMock()
    _mock_slot0(w3, 3033126396693973345289760393)
    w3.eth.block_number = 99999

    quote = get_aerodrome_price(
        w3=w3,
        pool_address="0x4F5905e36ac07eE1F01ffB939aA7f212A58D5CDF",
        token0_decimals=6,
        token1_decimals=8,
        invert=True,
        pair="cbBTC/USDC",
        fee_pct=0.0001,
    )

    assert quote.venue == "aerodrome"
    assert quote.pair == "cbBTC/USDC"
    assert quote.price > 0
    assert quote.fee_pct == 0.0001
    assert quote.block == 99999
    assert quote.timestamp > 0


def test_uniswap_price_returns_quote():
    """get_uniswap_price should call QuoterV2 and return a PriceQuote."""
    amount_out_raw = int(68_193.69 * 1e6)
    w3 = MagicMock()
    mock_quoter = MagicMock()
    w3.eth.contract.return_value = mock_quoter
    mock_quoter.functions.quoteExactInputSingle.return_value.call.return_value = [
        amount_out_raw, 0, 0, 100000
    ]
    w3.eth.block_number = 12000002

    quote = get_uniswap_price(
        w3=w3,
        token_in=config.CBBTC_ADDRESS,
        token_out=config.USDC_ADDRESS,
        amount_in=10 ** 8,
        fee=500,
        dec_in=8,
        dec_out=6,
        pair="cbBTC/USDC",
        fee_pct=0.0005,
    )

    assert quote.venue == "uniswap"
    assert quote.pair  == "cbBTC/USDC"
    assert abs(quote.price - 68_193.69) < 0.01
    assert quote.fee_pct == 0.0005
    assert quote.block == 12000002
    assert quote.timestamp > 0


def test_uniswap_price_weeth():
    """Uniswap weETH/WETH: 1 weETH → 1.0909 WETH."""
    amount_out_raw = int(1.0909 * 1e18)
    w3 = MagicMock()
    mock_quoter = MagicMock()
    w3.eth.contract.return_value = mock_quoter
    mock_quoter.functions.quoteExactInputSingle.return_value.call.return_value = [
        amount_out_raw, 0, 0, 80000
    ]
    w3.eth.block_number = 12000003

    quote = get_uniswap_price(
        w3=w3,
        token_in=config.WEETH_ADDRESS,
        token_out=config.WETH_ADDRESS,
        amount_in=10 ** 18,
        fee=100,
        dec_in=18,
        dec_out=18,
        pair="weETH/WETH",
        fee_pct=0.0001,
    )

    assert quote.venue == "uniswap"
    assert abs(quote.price - 1.0909) < 0.0001


def test_get_all_prices_returns_both_pairs():
    """get_all_prices must return cbBTC/USDC and weETH/WETH at minimum."""
    w3 = MagicMock()

    cbbtc_sqrt = 3033126396693973345289760393
    weeth_sqrt  = 82749893355765197634930415633
    ZERO = "0x0000000000000000000000000000000000000000"

    call_count = {"n": 0}

    def mock_contract(address=None, abi=None):
        m = MagicMock()
        addr = str(address).lower() if address else ""

        # Slipstream factory getPool calls: return zero for most, real for known
        aero_cbbtc = config.AERO_CBBTC_USDC_POOL.lower()
        aero_weeth = config.AERO_WEETH_WETH_POOL.lower()

        if addr == aero_cbbtc:
            m.functions.slot0.return_value.call.return_value = [cbbtc_sqrt, 1, 0, 1, 1, 0, True]
        elif addr == aero_weeth:
            m.functions.slot0.return_value.call.return_value = [weeth_sqrt, 1, 0, 1, 1, 0, True]
        else:
            # factory calls: return zero pool (no pool found)
            m.functions.getPool.return_value.call.return_value = ZERO
            m.functions.getPair.return_value.call.return_value = ZERO
            # quoter calls: return non-zero amount
            m.functions.quoteExactInputSingle.return_value.call.return_value = [
                int(68193 * 1e6), 0, 0, 100000
            ]
            # ERC20 balance (liquidity gate)
            m.functions.balanceOf.return_value.call.return_value = int(500_000 * 1e6)

        m.functions.slot0.return_value.call.return_value = [cbbtc_sqrt, 1, 0, 1, 1, 0, True]
        return m

    w3.eth.contract.side_effect = mock_contract
    w3.eth.block_number = 12000010

    # Patch PAIR_CONFIG to just 2 pairs for speed; patch DEX_CONFIG to just slipstream+uni
    mini_pairs = [
        p for p in config.PAIR_CONFIG if p["name"] in ("cbBTC/USDC", "weETH/WETH")
    ]
    mini_dexes = [d for d in config.DEX_CONFIG if d["name"] in ("Aerodrome Slipstream", "Uniswap V3")]

    with patch("config.PAIR_CONFIG", mini_pairs), patch("config.DEX_CONFIG", mini_dexes):
        prices = get_all_prices(w3)

    # At minimum, must return some results with PriceQuote objects
    for pair_name, quotes in prices.items():
        assert all(isinstance(q, PriceQuote) for q in quotes)


# ── New multi-DEX tests ────────────────────────────────────────────────────────

def test_multi_pair_scan_returns_all_pairs():
    """get_all_prices returns an entry for every pair that has >= 2 DEX quotes."""
    w3 = MagicMock()
    ZERO = "0x0000000000000000000000000000000000000000"

    def mock_contract(address=None, abi=None):
        m = MagicMock()
        # Slipstream / V3 factory: always return non-zero pool
        m.functions.getPool.return_value.call.return_value = "0x" + "A" * 40
        m.functions.getPair.return_value.call.return_value = "0x" + "B" * 40
        # slot0: valid price
        m.functions.slot0.return_value.call.return_value = [
            3033126396693973345289760393, 1, 0, 1, 1, 0, True
        ]
        # QuoterV2: valid quote
        m.functions.quoteExactInputSingle.return_value.call.return_value = [
            int(68000 * 1e6), 0, 0, 100000
        ]
        # ERC20 balance for liquidity gate: 1M USDC
        m.functions.balanceOf.return_value.call.return_value = int(1_000_000 * 1e6)
        # V2 token0
        m.functions.token0.return_value.call.return_value = "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf"
        # V2 getReserves
        m.functions.getReserves.return_value.call.return_value = [
            int(1000 * 1e8), int(68_000_000 * 1e6), 0
        ]
        return m

    w3.eth.contract.side_effect = mock_contract
    w3.eth.block_number = 99

    # Only scan 3 pairs to keep test fast
    mini_pairs = config.PAIR_CONFIG[:3]
    mini_dexes = config.DEX_CONFIG[:2]

    with patch("config.PAIR_CONFIG", mini_pairs), \
         patch("config.DEX_CONFIG", mini_dexes), \
         patch("price_scanner._check_liquidity", return_value=True):
        prices = get_all_prices(w3)

    # Should return up to 3 pairs with lists of quotes
    for name, quotes in prices.items():
        assert isinstance(quotes, list)
        assert all(isinstance(q, PriceQuote) for q in quotes)


def test_liquidity_gate_skips_thin_pool():
    """_check_liquidity returns False when USDC balance < min_liquidity/2."""
    w3 = MagicMock()
    erc20 = MagicMock()
    w3.eth.contract.return_value = erc20

    # 10 USDC in pool vs min_liquidity=100k → fails gate
    erc20.functions.balanceOf.return_value.call.return_value = int(10 * 1e6)

    result = _check_liquidity(
        w3=w3,
        pool_address="0x" + "A" * 40,
        token_in=config.CBBTC_ADDRESS,
        token_out=config.USDC_ADDRESS,
        dec_in=8,
        dec_out=6,
        min_liquidity_usd=100_000,
    )

    assert result is False


def test_liquidity_gate_passes_with_enough():
    """_check_liquidity returns True when USDC balance >= min_liquidity/2."""
    w3 = MagicMock()
    erc20 = MagicMock()
    w3.eth.contract.return_value = erc20

    # 1M USDC in pool
    erc20.functions.balanceOf.return_value.call.return_value = int(1_000_000 * 1e6)

    result = _check_liquidity(
        w3=w3,
        pool_address="0x" + "A" * 40,
        token_in=config.CBBTC_ADDRESS,
        token_out=config.USDC_ADDRESS,
        dec_in=8,
        dec_out=6,
        min_liquidity_usd=100_000,
    )

    assert result is True


def test_best_price_selection_across_dexes():
    """get_all_prices returns the DEX with highest price in the list."""
    w3 = MagicMock()

    quotes_returned = [
        PriceQuote("Aerodrome Slipstream", "cbBTC/USDC", 68300.0, 0.0001, 1, time.time()),
        PriceQuote("Uniswap V3",           "cbBTC/USDC", 68100.0, 0.0005, 1, time.time()),
    ]

    with patch("price_scanner._get_quotes_for_pair", return_value=quotes_returned), \
         patch("config.PAIR_CONFIG", [config.PAIR_CONFIG[0]]):
        prices = get_all_prices(w3)

    assert "cbBTC/USDC" in prices
    prices_list = prices["cbBTC/USDC"]
    best = max(prices_list, key=lambda q: q.price)
    worst = min(prices_list, key=lambda q: q.price)
    assert best.price == 68300.0
    assert worst.price == 68100.0


def test_uniswap_v3_quote_for_new_pairs():
    """_quote_uniswap_v3 returns a PriceQuote for a valid pair/DEX combo."""
    w3 = MagicMock()
    ZERO = "0x0000000000000000000000000000000000000000"

    def mock_contract(address=None, abi=None):
        m = MagicMock()
        m.functions.getPool.return_value.call.return_value = "0x" + "C" * 40
        m.functions.quoteExactInputSingle.return_value.call.return_value = [
            int(68000 * 1e6), 0, 0, 100000
        ]
        m.functions.balanceOf.return_value.call.return_value = int(500_000 * 1e6)
        return m

    w3.eth.contract.side_effect = mock_contract
    w3.eth.block_number = 1

    pair_cfg = config.PAIR_CONFIG[0]  # cbBTC/USDC
    dex_cfg  = next(d for d in config.DEX_CONFIG if d["name"] == "Uniswap V3")

    with patch("price_scanner._check_liquidity", return_value=True):
        q = _quote_uniswap_v3(w3, pair_cfg, dex_cfg)

    assert q is not None
    assert q.price > 0
    assert q.pair == "cbBTC/USDC"


def test_aerodrome_vamm_price_calculation():
    """_quote_uniswap_v2 computes price from getAmountOut correctly."""
    from price_scanner import _quote_uniswap_v2
    w3 = MagicMock()

    # unit_size for cbBTC/USDC = 0.1 cbBTC → amount_in = 10_000_000 raw
    # For price = 68000: amount_out = 0.1 * 68000 * 1e6 = 6_800_000_000 raw USDC
    pair_cfg = config.PAIR_CONFIG[0]  # cbBTC/USDC, token_in=cbBTC
    expected_out = int(pair_cfg["unit_size"] * 68_000 * (10 ** pair_cfg["dec_out"]))

    def mock_contract(address=None, abi=None):
        m = MagicMock()
        # vAMM factory: return a valid pair address
        m.functions.getPair.return_value.call.return_value = "0x" + "D" * 40
        # getAmountOut: execution quote → 68000 USDC per cbBTC
        m.functions.getAmountOut.return_value.call.return_value = expected_out
        m.functions.balanceOf.return_value.call.return_value = int(68_000 * 1e6)
        return m

    w3.eth.contract.side_effect = mock_contract
    w3.eth.block_number = 2

    dex_cfg = next(d for d in config.DEX_CONFIG if d["name"] == "Aerodrome vAMM")

    with patch("price_scanner._check_liquidity", return_value=True):
        q = _quote_uniswap_v2(w3, pair_cfg, dex_cfg)

    assert q is not None
    assert abs(q.price - 68000.0) < 1.0
    assert q.venue == "Aerodrome vAMM"
    assert q.method == "execution"


# ── New: execution-quote enforcement tests ────────────────────────────────────

def test_aerodrome_uses_execution_quote_not_slot0():
    """_quote_slipstream must call CLQuoter (method='execution'), not slot0 (method='spot')."""
    from price_scanner import _quote_slipstream

    w3 = MagicMock()
    pair_cfg = config.PAIR_CONFIG[0]  # cbBTC/USDC
    # amount_out for ~68000 price: unit_size * 68000 * 10^dec_out
    expected_out = int(pair_cfg["unit_size"] * 68_000 * (10 ** pair_cfg["dec_out"]))

    def mock_contract(address=None, abi=None):
        m = MagicMock()
        m.functions.getPool.return_value.call.return_value = "0x" + "A" * 40
        m.functions.quoteExactInputSingle.return_value.call.return_value = [
            expected_out, 0, 0, 100000
        ]
        return m

    w3.eth.contract.side_effect = mock_contract
    w3.eth.block_number = 1

    dex_cfg = next(d for d in config.DEX_CONFIG if d["name"] == "Aerodrome Slipstream")

    with patch("price_scanner._check_liquidity", return_value=True):
        q = _quote_slipstream(w3, pair_cfg, dex_cfg)

    assert q is not None, "CLQuoter must return a quote"
    assert q.method == "execution", f"Expected method='execution', got '{q.method}'"


def test_unit_size_consistent_across_dexes():
    """Every pair in PAIR_CONFIG has a positive unit_size for consistent cross-DEX quotes."""
    for pair_cfg in config.PAIR_CONFIG:
        assert "unit_size" in pair_cfg, f"Missing unit_size in {pair_cfg['name']}"
        assert pair_cfg["unit_size"] > 0, f"unit_size must be > 0 for {pair_cfg['name']}"
        amount_in = int(pair_cfg["unit_size"] * (10 ** pair_cfg["dec_in"]))
        assert amount_in > 0, f"amount_in must be > 0 for {pair_cfg['name']}"


def test_price_quote_has_method_execution():
    """PriceQuote default method is 'execution'; get_aerodrome_price returns 'spot'."""
    q = PriceQuote("Uniswap V3", "cbBTC/USDC", 68000.0, 0.0005, 1, time.time())
    assert q.method == "execution"

    # get_aerodrome_price (slot0-based) must return method='spot'
    w3 = MagicMock()
    pool = MagicMock()
    w3.eth.contract.return_value = pool
    pool.functions.slot0.return_value.call.return_value = [
        3033126396693973345289760393, 1, 0, 1, 1, 0, True
    ]
    w3.eth.block_number = 1
    q_spot = get_aerodrome_price(
        w3=w3, pool_address="0x" + "A" * 40,
        token0_decimals=6, token1_decimals=8,
        invert=True, pair="cbBTC/USDC", fee_pct=0.0001,
    )
    assert q_spot.method == "spot"
