"""
Tests for price_scanner.py — all offline using mocked web3 calls.
"""

import time
import pytest
from unittest.mock import MagicMock, patch, PropertyMock

from price_scanner import (
    PriceQuote,
    get_aerodrome_price,
    get_uniswap_price,
    get_all_prices,
)
import config


# ── Aerodrome price math tests ─────────────────────────────────────────────────

def test_aerodrome_price_math_cbbtc():
    """
    sqrtPriceX96=3033126396693973345289760393
    pool: token0=USDC(6dec), token1=cbBTC(8dec), invert=True
    Expected: USDC per cbBTC ≈ $68,230 ± $100

    Math:
      Q96 = 2^96
      ratio = sqrtPriceX96 / Q96 = 3033126396693973345289760393 / 79228162514264337593543950336
      raw_price = ratio^2  (cbBTC_raw per USDC_raw)
      dec_adjust = 1e6 / 1e8 = 0.01
      price_t1_per_t0 = raw_price * 0.01  (cbBTC per USDC, human)
      invert → USDC per cbBTC
    """
    sqrt_price_x96 = 3033126396693973345289760393

    w3 = MagicMock()
    mock_pool = MagicMock()
    w3.eth.contract.return_value = mock_pool
    mock_pool.functions.slot0.return_value.call.return_value = [
        sqrt_price_x96, 1, 0, 1, 1, 0, True
    ]
    w3.eth.block_number = 12000000

    quote = get_aerodrome_price(
        w3=w3,
        pool_address="0x4F5905e36ac07eE1F01ffB939aA7f212A58D5CDF",
        token0_decimals=6,   # USDC
        token1_decimals=8,   # cbBTC
        invert=True,
        pair="cbBTC/USDC",
        fee_pct=0.0001,
    )

    assert quote.venue == "aerodrome"
    assert quote.pair == "cbBTC/USDC"
    assert 68_130 <= quote.price <= 68_330, f"Expected ~68,230 ± 100, got {quote.price:.2f}"
    assert quote.fee_pct == 0.0001
    assert quote.block == 12000000


def test_aerodrome_price_math_weeth():
    """
    sqrtPriceX96=82749893355765197634930415633
    pool: token0=weETH(18dec), token1=WETH(18dec), invert=False
    Expected: WETH per weETH ≈ 1.0909 ± 0.001

    Math:
      ratio = sqrtPriceX96 / 2^96
      raw_price = ratio^2
      dec_adjust = 1e18/1e18 = 1.0
      price = raw_price * 1.0 ≈ 1.0909
    """
    sqrt_price_x96 = 82749893355765197634930415633

    w3 = MagicMock()
    mock_pool = MagicMock()
    w3.eth.contract.return_value = mock_pool
    mock_pool.functions.slot0.return_value.call.return_value = [
        sqrt_price_x96, 1, 0, 1, 1, 0, True
    ]
    w3.eth.block_number = 12000001

    quote = get_aerodrome_price(
        w3=w3,
        pool_address="0xbD3cd0D9d429b41F0a2e1C026552Bd598294d5E0",
        token0_decimals=18,  # weETH
        token1_decimals=18,  # WETH
        invert=False,
        pair="weETH/WETH",
        fee_pct=0.0001,
    )

    assert quote.venue == "aerodrome"
    assert quote.pair == "weETH/WETH"
    assert 1.0899 <= quote.price <= 1.0919, f"Expected ~1.0909 ± 0.001, got {quote.price:.6f}"


def test_aerodrome_price_quote_fields_populated():
    """All PriceQuote fields must be populated."""
    sqrt_price_x96 = 3033126396693973345289760393
    w3 = MagicMock()
    mock_pool = MagicMock()
    w3.eth.contract.return_value = mock_pool
    mock_pool.functions.slot0.return_value.call.return_value = [
        sqrt_price_x96, 1, 0, 1, 1, 0, True
    ]
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
    # Mock: 1 cbBTC (1e8) in → 68_193.69 USDC out (6 dec = 68_193_690_000 raw)
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
        amount_in=10 ** 8,   # 1 cbBTC
        fee=500,
        dec_in=8,
        dec_out=6,
        pair="cbBTC/USDC",
        fee_pct=0.0005,
    )

    assert quote.venue == "uniswap"
    assert quote.pair == "cbBTC/USDC"
    assert abs(quote.price - 68_193.69) < 0.01, f"Expected ~68193.69, got {quote.price:.2f}"
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
    """get_all_prices must return both cbBTC/USDC and weETH/WETH."""
    w3 = MagicMock()

    cbbtc_sqrt = 3033126396693973345289760393
    weeth_sqrt  = 82749893355765197634930415633

    def mock_contract(address, abi):
        m = MagicMock()
        if address in (config.AERO_CBBTC_USDC_POOL, config.AERO_WEETH_WETH_POOL):
            # Aerodrome pool mock
            if address == config.AERO_CBBTC_USDC_POOL:
                m.functions.slot0.return_value.call.return_value = [cbbtc_sqrt, 1, 0, 1, 1, 0, True]
            else:
                m.functions.slot0.return_value.call.return_value = [weeth_sqrt, 1, 0, 1, 1, 0, True]
        else:
            # Quoter mock
            m.functions.quoteExactInputSingle.return_value.call.return_value = [
                int(68193 * 1e6), 0, 0, 100000
            ]
        return m

    w3.eth.contract.side_effect = mock_contract
    w3.eth.block_number = 12000010

    prices = get_all_prices(w3)

    assert "cbBTC/USDC" in prices
    assert "weETH/WETH" in prices
    aero_q, uni_q = prices["cbBTC/USDC"]
    assert isinstance(aero_q, PriceQuote)
    assert isinstance(uni_q, PriceQuote)
    assert aero_q.venue == "aerodrome"
    assert uni_q.venue == "uniswap"
