"""
Price scanner for DEX Arbitrage Bot.
Reads prices from Aerodrome (slot0 sqrtPriceX96) and Uniswap V3 (QuoterV2).
Uses CDP RPC for all reads — never uses Alchemy for price queries.
Never imports from the morpho_scanner liquidation bot.
"""

import logging
import time
from dataclasses import dataclass, field

from web3 import Web3

import config

logger = logging.getLogger(__name__)

# ── ABIs ──────────────────────────────────────────────────────────────────────

_SLOT0_ABI = [
    {
        "inputs": [],
        "name": "slot0",
        "outputs": [
            {"name": "sqrtPriceX96",              "type": "uint160"},
            {"name": "tick",                      "type": "int24"},
            {"name": "observationIndex",          "type": "uint16"},
            {"name": "observationCardinality",    "type": "uint16"},
            {"name": "observationCardinalityNext","type": "uint16"},
            {"name": "unlocked",                  "type": "bool"},
        ],
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

_FACTORY_ABI = [
    {
        "inputs": [
            {"name": "tokenA", "type": "address"},
            {"name": "tokenB", "type": "address"},
            {"name": "fee",    "type": "uint24"},
        ],
        "name": "getPool",
        "outputs": [{"name": "pool", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    }
]

# Uniswap pool ABI (same slot0 structure)
_UNI_POOL_ABI = _SLOT0_ABI


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class PriceQuote:
    venue: str         # "aerodrome" or "uniswap"
    pair: str          # e.g. "cbBTC/USDC"
    price: float       # human-readable, e.g. 68297.04 USDC per cbBTC
    fee_pct: float     # 0.0001 or 0.0005
    block: int
    timestamp: float   # unix timestamp

    def __repr__(self) -> str:
        return (
            f"PriceQuote({self.venue} {self.pair} "
            f"price={self.price:.4f} fee={self.fee_pct*100:.3f}% "
            f"block={self.block})"
        )


# ── Aerodrome price reader ─────────────────────────────────────────────────────

def get_aerodrome_price(
    w3: Web3,
    pool_address: str,
    token0_decimals: int,
    token1_decimals: int,
    invert: bool = False,
    pair: str = "",
    fee_pct: float = 0.0001,
) -> PriceQuote:
    """
    Read slot0 from an Aerodrome (Uniswap V3 fork) pool and return a PriceQuote.

    The sqrtPriceX96 encodes price as: price = (sqrtPriceX96 / 2^96)^2
    where price = token1 per token0 (in raw units).

    For cbBTC/USDC pool (token0=USDC 6dec, token1=cbBTC 8dec):
        raw_price = (sqrtPriceX96 / 2^96)^2
        cbbtc_per_usdc_raw = raw_price                 # cbBTC_raw per USDC_raw
        cbbtc_per_usdc = raw_price * 1e6 / 1e8         # adjust decimals
        usdc_per_cbbtc = 1 / cbbtc_per_usdc            # invert

    For weETH/WETH pool (token0=weETH 18dec, token1=WETH 18dec):
        raw_price = (sqrtPriceX96 / 2^96)^2
        weth_per_weeth = raw_price * 1e18 / 1e18 = raw_price
        weeth_per_weth = 1 / raw_price (if invert=True)

    Args:
        pool_address: Aerodrome pool contract address
        token0_decimals: decimal places for token0
        token1_decimals: decimal places for token1
        invert: if True, return price of token0 in terms of token1
        pair: human label (e.g. "cbBTC/USDC")
        fee_pct: pool fee as fraction (e.g. 0.0001 for 0.01%)
    """
    pool = w3.eth.contract(
        address=Web3.to_checksum_address(pool_address),
        abi=_SLOT0_ABI,
    )
    slot0 = pool.functions.slot0().call()
    sqrt_price_x96 = slot0[0]
    block = w3.eth.block_number

    # price = (sqrtPriceX96 / 2^96)^2 → token1_raw per token0_raw
    Q96 = 2 ** 96
    ratio = sqrt_price_x96 / Q96
    raw_price = ratio * ratio  # token1_raw per token0_raw

    # Adjust for decimals: token1_human per token0_human
    # raw_price = (token1_raw / token0_raw)
    # human_price = raw_price * 10^token0_decimals / 10^token1_decimals
    dec_adjustment = (10 ** token0_decimals) / (10 ** token1_decimals)
    price_t1_per_t0 = raw_price * dec_adjustment  # token1 per token0 (human)

    if invert:
        price = 1.0 / price_t1_per_t0 if price_t1_per_t0 > 0 else 0.0
    else:
        price = price_t1_per_t0

    return PriceQuote(
        venue="aerodrome",
        pair=pair,
        price=price,
        fee_pct=fee_pct,
        block=block,
        timestamp=time.time(),
    )


# ── Uniswap V3 price reader ───────────────────────────────────────────────────

def get_uniswap_price(
    w3: Web3,
    token_in: str,
    token_out: str,
    amount_in: int,   # raw units
    fee: int,         # e.g. 500
    dec_in: int,
    dec_out: int,
    pair: str = "",
    fee_pct: float = 0.0005,
) -> PriceQuote:
    """
    Call QuoterV2.quoteExactInputSingle() and return a PriceQuote.
    amount_in should be 1 unit of token_in in raw (e.g. 1e8 for 1 cbBTC).
    Returns price as (amount_out_human / amount_in_human).
    """
    quoter = w3.eth.contract(
        address=config.UNISWAP_QUOTER_V2,
        abi=_QUOTER_V2_ABI,
    )
    result = quoter.functions.quoteExactInputSingle({
        "tokenIn":           Web3.to_checksum_address(token_in),
        "tokenOut":          Web3.to_checksum_address(token_out),
        "amountIn":          amount_in,
        "fee":               fee,
        "sqrtPriceLimitX96": 0,
    }).call()

    amount_out_raw = result[0]
    block = w3.eth.block_number

    # Convert raw amounts to human-readable price
    amount_in_human  = amount_in / (10 ** dec_in)
    amount_out_human = amount_out_raw / (10 ** dec_out)
    price = amount_out_human / amount_in_human if amount_in_human > 0 else 0.0

    return PriceQuote(
        venue="uniswap",
        pair=pair,
        price=price,
        fee_pct=fee_pct,
        block=block,
        timestamp=time.time(),
    )


# ── Pool lookup via factory ────────────────────────────────────────────────────

def get_uniswap_pool(w3: Web3, token_a: str, token_b: str, fee: int) -> str:
    """Look up Uniswap V3 pool address via factory.getPool()."""
    factory = w3.eth.contract(
        address=config.UNISWAP_FACTORY,
        abi=_FACTORY_ABI,
    )
    pool_addr = factory.functions.getPool(
        Web3.to_checksum_address(token_a),
        Web3.to_checksum_address(token_b),
        fee,
    ).call()
    return pool_addr


# ── Aggregate price fetcher ────────────────────────────────────────────────────

def get_all_prices(w3: Web3) -> dict:
    """
    Fetch prices from both Aerodrome and Uniswap for all monitored pairs.

    Returns:
        {
            "cbBTC/USDC": (aero_quote, uni_quote),
            "weETH/WETH": (aero_quote, uni_quote),
        }

    Aerodrome cbBTC/USDC pool: token0=USDC(6dec), token1=cbBTC(8dec)
        slot0 price = cbBTC_raw per USDC_raw
        We want USDC per cbBTC → invert=True after decimal adjustment

    Aerodrome weETH/WETH pool: token0=weETH(18dec), token1=WETH(18dec)
        slot0 price = WETH per weETH (token1 per token0)
        We want WETH per weETH → invert=False
    """
    results = {}

    # ── cbBTC/USDC ────────────────────────────────────────────────────────────
    # Aerodrome: token0=USDC(6), token1=cbBTC(8)
    # slot0 raw price = cbBTC_raw per USDC_raw
    # after decimal adjustment: cbBTC per USDC (human)
    # invert → USDC per cbBTC (human) ✓
    aero_cbbtc = get_aerodrome_price(
        w3=w3,
        pool_address=config.AERO_CBBTC_USDC_POOL,
        token0_decimals=config.USDC_DECIMALS,
        token1_decimals=config.CBBTC_DECIMALS,
        invert=True,
        pair="cbBTC/USDC",
        fee_pct=config.AERODROME_FEE_CBBTC_USDC,
    )

    # Uniswap: sell 1 cbBTC → receive USDC
    uni_cbbtc = get_uniswap_price(
        w3=w3,
        token_in=config.CBBTC_ADDRESS,
        token_out=config.USDC_ADDRESS,
        amount_in=10 ** config.CBBTC_DECIMALS,  # 1 cbBTC in raw
        fee=config.UNISWAP_FEE_CBBTC_USDC,
        dec_in=config.CBBTC_DECIMALS,
        dec_out=config.USDC_DECIMALS,
        pair="cbBTC/USDC",
        fee_pct=config.UNISWAP_FEE_PCT_CBBTC_USDC,
    )
    results["cbBTC/USDC"] = (aero_cbbtc, uni_cbbtc)

    # ── weETH/WETH ────────────────────────────────────────────────────────────
    # Aerodrome: token0=weETH(18), token1=WETH(18)
    # slot0 price = WETH per weETH → keep as is (invert=False)
    aero_weeth = get_aerodrome_price(
        w3=w3,
        pool_address=config.AERO_WEETH_WETH_POOL,
        token0_decimals=config.WEETH_DECIMALS,
        token1_decimals=config.WETH_DECIMALS,
        invert=False,
        pair="weETH/WETH",
        fee_pct=config.AERODROME_FEE_WEETH_WETH,
    )

    # Uniswap: sell 1 weETH → receive WETH
    uni_weeth = get_uniswap_price(
        w3=w3,
        token_in=config.WEETH_ADDRESS,
        token_out=config.WETH_ADDRESS,
        amount_in=10 ** config.WEETH_DECIMALS,  # 1 weETH in raw
        fee=config.UNISWAP_FEE_WEETH_WETH,
        dec_in=config.WEETH_DECIMALS,
        dec_out=config.WETH_DECIMALS,
        pair="weETH/WETH",
        fee_pct=config.UNISWAP_FEE_PCT_WEETH_WETH,
    )
    results["weETH/WETH"] = (aero_weeth, uni_weeth)

    return results
