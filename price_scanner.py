"""
Price scanner for DEX Arbitrage Bot.
Reads execution quotes from Aerodrome Slipstream (CLQuoter), Uniswap V3 forks
(QuoterV2), and Aerodrome vAMM (getAmountOut). Supports 15 pairs × 5 DEXes.
All quotes are execution quotes (include slippage for unit_size tokens).
Uses CDP RPC for all reads — never uses Alchemy for price queries.
Never imports from the morpho_scanner liquidation bot.
"""

import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import List, Optional

from eth_abi import decode as abi_decode, encode as abi_encode
from web3 import Web3

import config
from utils.multicall import multicall3

logger = logging.getLogger(__name__)

# ── ABIs ──────────────────────────────────────────────────────────────────────

_SLOT0_ABI = [
    {
        "inputs": [],
        "name": "slot0",
        "outputs": [
            {"name": "sqrtPriceX96",               "type": "uint160"},
            {"name": "tick",                        "type": "int24"},
            {"name": "observationIndex",            "type": "uint16"},
            {"name": "observationCardinality",      "type": "uint16"},
            {"name": "observationCardinalityNext",  "type": "uint16"},
            {"name": "unlocked",                    "type": "bool"},
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

# Aerodrome Slipstream CLQuoter — uses tickSpacing (int24) instead of fee (uint24).
# Interface: quoteExactInputSingle({tokenIn, tokenOut, amountIn, tickSpacing, sqrtPriceLimitX96})
_SLIPSTREAM_QUOTER_ABI = [
    {
        "inputs": [
            {
                "components": [
                    {"name": "tokenIn",           "type": "address"},
                    {"name": "tokenOut",          "type": "address"},
                    {"name": "amountIn",          "type": "uint256"},
                    {"name": "tickSpacing",       "type": "int24"},
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

_FACTORY_V3_ABI = [
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

# Aerodrome Slipstream factory uses tickSpacing instead of fee
_FACTORY_SLIPSTREAM_ABI = [
    {
        "inputs": [
            {"name": "tokenA",      "type": "address"},
            {"name": "tokenB",      "type": "address"},
            {"name": "tickSpacing", "type": "int24"},
        ],
        "name": "getPool",
        "outputs": [{"name": "pool", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    }
]

# Aerodrome v1 vAMM factory
_FACTORY_V2_ABI = [
    {
        "inputs": [
            {"name": "tokenA", "type": "address"},
            {"name": "tokenB", "type": "address"},
            {"name": "stable", "type": "bool"},
        ],
        "name": "getPair",
        "outputs": [{"name": "pair", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    }
]

_POOL_V2_ABI = [
    {
        "inputs": [],
        "name": "getReserves",
        "outputs": [
            {"name": "_reserve0", "type": "uint112"},
            {"name": "_reserve1", "type": "uint112"},
            {"name": "_blockTimestampLast", "type": "uint32"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "token0",
        "outputs": [{"name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    # getAmountOut — execution quote that includes the vAMM invariant and fee.
    # Replaces the spot-price reserves ratio for arb detection.
    {
        "inputs": [
            {"name": "amountIn", "type": "uint256"},
            {"name": "tokenIn",  "type": "address"},
        ],
        "name": "getAmountOut",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]

_ERC20_BALANCE_ABI = [
    {
        "inputs": [{"name": "account", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    }
]

# Tick spacing → fee_pct mapping for Aerodrome Slipstream pools
_TICK_SPACING_FEE: dict = {
    1:   0.0001,   # 0.01%
    50:  0.0005,   # 0.05%
    100: 0.003,    # 0.30%
    200: 0.01,     # 1.00%
}

# Uniswap V3 fee tier → fee_pct
_FEE_TIER_PCT: dict = {
    100:   0.0001,
    500:   0.0005,
    2500:  0.0025,
    3000:  0.003,
    10000: 0.01,
}

# Rough ETH price for liquidity gate (WETH-denominated pools)
_ETH_PRICE_ROUGH_USD: float = 3500.0

# Zero address constant
_ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

# ── Multicall3 pool cache ──────────────────────────────────────────────────────
# key: (pair_name, dex_name, fee_or_tick)  →  pool_address (str) | None
# "fee_or_tick" is an int (fee tier or tick spacing) for V3/slipstream,
# or the string "v2" for Aerodrome vAMM (one pool per pair, no fee param).
_pool_cache: dict = {}
_pool_cache_initialized: bool = False

# Pre-computed 4-byte function selectors for ABI calldata encoding.
# Computed once at module load; no RPC call needed.
_SEL_GETPOOL_TICK = Web3.keccak(text="getPool(address,address,int24)")[:4]
_SEL_GETPOOL_FEE  = Web3.keccak(text="getPool(address,address,uint24)")[:4]
_SEL_GETPAIR      = Web3.keccak(text="getPair(address,address,bool)")[:4]
_SEL_QUOTE_SLIP   = Web3.keccak(
    text="quoteExactInputSingle((address,address,uint256,int24,uint160))"
)[:4]
_SEL_QUOTE_V3     = Web3.keccak(
    text="quoteExactInputSingle((address,address,uint256,uint24,uint160))"
)[:4]
_SEL_AMOUNT_OUT   = Web3.keccak(text="getAmountOut(uint256,address)")[:4]


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class PriceQuote:
    venue: str         # e.g. "Aerodrome Slipstream" or "Uniswap V3"
    pair: str          # e.g. "cbBTC/USDC"
    price: float       # human-readable price: token_out per token_in
    fee_pct: float     # e.g. 0.0001 for 0.01%
    block: int
    timestamp: float
    method: str = "execution"  # "execution" = quoter-based (includes slippage)
                               # "spot"      = slot0 or reserves ratio (no slippage)

    def __repr__(self) -> str:
        return (
            f"PriceQuote({self.venue} {self.pair} "
            f"price={self.price:.6f} fee={self.fee_pct*100:.3f}% "
            f"method={self.method} block={self.block})"
        )


# ── Aerodrome price reader (slot0 / Slipstream) ───────────────────────────────

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
    Read slot0 from an Aerodrome Slipstream (Uniswap V3 fork) pool.

    sqrtPriceX96 encodes: price = (sqrtPriceX96 / 2^96)^2  (token1_raw per token0_raw)

    For cbBTC/USDC pool (token0=USDC 6dec, token1=cbBTC 8dec):
        raw = ratio^2;  human = raw * 1e6 / 1e8;  invert → USDC per cbBTC
    For weETH/WETH pool (both 18dec):
        human = raw;  no invert → WETH per weETH
    """
    pool = w3.eth.contract(
        address=Web3.to_checksum_address(pool_address),
        abi=_SLOT0_ABI,
    )
    slot0 = pool.functions.slot0().call()
    sqrt_price_x96 = slot0[0]
    block = w3.eth.block_number

    Q96 = 2 ** 96
    ratio = sqrt_price_x96 / Q96
    raw_price = ratio * ratio  # token1_raw per token0_raw

    # Adjust for decimal difference: token1_human per token0_human
    dec_adj = (10 ** token0_decimals) / (10 ** token1_decimals)
    price_t1_per_t0 = raw_price * dec_adj

    price = (1.0 / price_t1_per_t0) if (invert and price_t1_per_t0 > 0) else price_t1_per_t0

    return PriceQuote(
        venue="aerodrome",
        pair=pair,
        price=price,
        fee_pct=fee_pct,
        block=block,
        timestamp=time.time(),
        method="spot",  # slot0-based: no slippage, not an execution quote
    )


# ── Uniswap V3 price reader (QuoterV2) ───────────────────────────────────────

def get_uniswap_price(
    w3: Web3,
    token_in: str,
    token_out: str,
    amount_in: int,
    fee: int,
    dec_in: int,
    dec_out: int,
    pair: str = "",
    fee_pct: float = 0.0005,
    quoter_address: str = "",
) -> PriceQuote:
    """
    Call QuoterV2.quoteExactInputSingle() and return price as token_out per token_in.
    Uses config.UNISWAP_QUOTER_V2 by default; override with quoter_address for other DEXes.
    """
    addr = quoter_address if quoter_address else config.UNISWAP_QUOTER_V2
    quoter = w3.eth.contract(
        address=Web3.to_checksum_address(addr),
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
        method="execution",
    )


# ── Pool lookup helpers ───────────────────────────────────────────────────────

def get_uniswap_pool(w3: Web3, token_a: str, token_b: str, fee: int,
                     factory: str = "") -> str:
    """Look up a Uniswap V3-style pool via factory.getPool()."""
    factory_addr = factory if factory else config.UNISWAP_FACTORY
    f = w3.eth.contract(
        address=Web3.to_checksum_address(factory_addr),
        abi=_FACTORY_V3_ABI,
    )
    return f.functions.getPool(
        Web3.to_checksum_address(token_a),
        Web3.to_checksum_address(token_b),
        fee,
    ).call()


def _get_slipstream_pool(w3: Web3, token_a: str, token_b: str,
                         tick_spacing: int, factory: str) -> str:
    """Look up an Aerodrome Slipstream pool via factory.getPool(tokenA, tokenB, tickSpacing)."""
    f = w3.eth.contract(
        address=Web3.to_checksum_address(factory),
        abi=_FACTORY_SLIPSTREAM_ABI,
    )
    return f.functions.getPool(
        Web3.to_checksum_address(token_a),
        Web3.to_checksum_address(token_b),
        tick_spacing,
    ).call()


def _get_v2_pair(w3: Web3, token_a: str, token_b: str,
                 factory: str, stable: bool = False) -> str:
    """Look up an Aerodrome vAMM pair via factory.getPair()."""
    f = w3.eth.contract(
        address=Web3.to_checksum_address(factory),
        abi=_FACTORY_V2_ABI,
    )
    return f.functions.getPair(
        Web3.to_checksum_address(token_a),
        Web3.to_checksum_address(token_b),
        stable,
    ).call()


# ── Liquidity gate ────────────────────────────────────────────────────────────

def _check_liquidity(
    w3: Web3,
    pool_address: str,
    token_in: str,
    token_out: str,
    dec_in: int,
    dec_out: int,
    min_liquidity_usd: float,
) -> bool:
    """
    Check pool has sufficient liquidity before quoting.
    Reads ERC20 balanceOf(pool) for the USDC/WETH side and estimates USD value.
    Returns True if liquidity >= min_liquidity_usd / 2.
    """
    USDC  = config.USDC_ADDRESS.lower()
    USDBC = config.USDBC_ADDRESS.lower()
    WETH  = config.WETH_ADDRESS.lower()

    ti = token_in.lower()
    to = token_out.lower()

    # Prefer checking the stablecoin or WETH side for USD estimation
    if to in (USDC, USDBC):
        check_token, check_dec = token_out, dec_out
        multiplier = 1.0
    elif ti in (USDC, USDBC):
        check_token, check_dec = token_in, dec_in
        multiplier = 1.0
    elif to == WETH:
        check_token, check_dec = token_out, dec_out
        multiplier = _ETH_PRICE_ROUGH_USD
    elif ti == WETH:
        check_token, check_dec = token_in, dec_in
        multiplier = _ETH_PRICE_ROUGH_USD
    else:
        # Unknown pair — skip the liquidity gate (assume passes)
        return True

    try:
        erc20 = w3.eth.contract(
            address=Web3.to_checksum_address(check_token),
            abi=_ERC20_BALANCE_ABI,
        )
        raw = erc20.functions.balanceOf(
            Web3.to_checksum_address(pool_address)
        ).call()
        balance_usd = (raw / (10 ** check_dec)) * multiplier
        return balance_usd >= min_liquidity_usd / 2.0
    except Exception:
        return True  # on error, don't block — let the quote call fail naturally


# ── Per-DEX quote fetchers ────────────────────────────────────────────────────

def _quote_slipstream(
    w3: Web3,
    pair_cfg: dict,
    dex_cfg: dict,
) -> Optional[PriceQuote]:
    """
    Price a pair on Aerodrome Slipstream using the CLQuoter (execution quote).

    Replaces the old slot0/sqrtPriceX96 approach which returned a spot price
    with no slippage — incomparable against V3 QuoterV2 execution quotes.

    Tries all tick_spacings; returns the highest-priced execution quote.
    unit_size from pair_cfg is used so all DEX adapters quote identical amounts.
    """
    token_in     = pair_cfg["token_in"]
    token_out    = pair_cfg["token_out"]
    dec_in       = pair_cfg["dec_in"]
    dec_out      = pair_cfg["dec_out"]
    unit_size    = pair_cfg["unit_size"]
    amount_in    = int(unit_size * (10 ** dec_in))
    factory      = dex_cfg["factory"]
    quoter_addr  = dex_cfg.get("quoter") or config.AERODROME_SLIPSTREAM_QUOTER
    best: Optional[PriceQuote] = None

    for ts in dex_cfg.get("tick_spacings", [1, 50, 100, 200]):
        # Step 1: find the pool (needed only for the liquidity gate)
        try:
            pool_addr = _get_slipstream_pool(w3, token_in, token_out, ts, factory)
        except Exception:
            continue
        if not pool_addr or pool_addr == _ZERO_ADDRESS:
            continue
        if not _check_liquidity(w3, pool_addr, token_in, token_out, dec_in, dec_out,
                                 pair_cfg["min_liquidity_usd"]):
            continue

        # Step 2: call CLQuoter for an execution quote (tickSpacing, not fee)
        try:
            fee = _TICK_SPACING_FEE.get(ts, dex_cfg.get("fee_pct", 0.0001))
            quoter = w3.eth.contract(
                address=Web3.to_checksum_address(quoter_addr),
                abi=_SLIPSTREAM_QUOTER_ABI,
            )
            result = quoter.functions.quoteExactInputSingle({
                "tokenIn":           Web3.to_checksum_address(token_in),
                "tokenOut":          Web3.to_checksum_address(token_out),
                "amountIn":          amount_in,
                "tickSpacing":       ts,
                "sqrtPriceLimitX96": 0,
            }).call()

            amount_out_raw   = result[0]
            block            = w3.eth.block_number
            amount_in_human  = amount_in / (10 ** dec_in)
            amount_out_human = amount_out_raw / (10 ** dec_out)
            price = amount_out_human / amount_in_human if amount_in_human > 0 else 0.0

            if price <= 0:
                continue

            q = PriceQuote(
                venue=dex_cfg["name"],
                pair=pair_cfg["name"],
                price=price,
                fee_pct=fee,
                block=block,
                timestamp=time.time(),
                method="execution",
            )
            if best is None or q.price > best.price:
                best = q

        except Exception as e:
            logger.debug("slipstream quoter failed ts=%d pair=%s: %s",
                         ts, pair_cfg["name"], e)

    return best


def _quote_uniswap_v3(
    w3: Web3,
    pair_cfg: dict,
    dex_cfg: dict,
) -> Optional[PriceQuote]:
    """
    Price a pair on a Uniswap V3-style DEX.
    Tries all fee_tiers; returns best (highest) quote.
    """
    token_in  = pair_cfg["token_in"]
    token_out = pair_cfg["token_out"]
    dec_in    = pair_cfg["dec_in"]
    dec_out   = pair_cfg["dec_out"]
    unit_size = pair_cfg["unit_size"]
    amount_in = int(unit_size * (10 ** dec_in))
    quoter    = dex_cfg.get("quoter", "")
    factory   = dex_cfg["factory"]
    best: Optional[PriceQuote] = None

    for fee in dex_cfg.get("fee_tiers", [500]):
        try:
            pool_addr = get_uniswap_pool(w3, token_in, token_out, fee, factory)
        except Exception:
            continue
        if not pool_addr or pool_addr == _ZERO_ADDRESS:
            continue
        if not _check_liquidity(w3, pool_addr, token_in, token_out, dec_in, dec_out,
                                 pair_cfg["min_liquidity_usd"]):
            continue
        try:
            fee_pct = _FEE_TIER_PCT.get(fee, fee / 1_000_000)
            q = get_uniswap_price(
                w3=w3,
                token_in=token_in,
                token_out=token_out,
                amount_in=amount_in,
                fee=fee,
                dec_in=dec_in,
                dec_out=dec_out,
                pair=pair_cfg["name"],
                fee_pct=fee_pct,
                quoter_address=quoter,
            )
            q.venue = dex_cfg["name"]
            if q.price > 0 and (best is None or q.price > best.price):
                best = q
        except Exception as e:
            logger.debug("v3 quote failed fee=%d pair=%s dex=%s: %s",
                         fee, pair_cfg["name"], dex_cfg["name"], e)

    return best


def _quote_uniswap_v2(
    w3: Web3,
    pair_cfg: dict,
    dex_cfg: dict,
) -> Optional[PriceQuote]:
    """
    Price a pair on Aerodrome vAMM using getAmountOut() (execution quote).

    Replaces the old reserves-ratio approach which returned a spot price with no
    slippage — incomparable against V3 QuoterV2 execution quotes.

    getAmountOut(amountIn, tokenIn) returns the actual tokens out including the
    vAMM invariant and the 0.02% swap fee.
    unit_size from pair_cfg is used so all DEX adapters quote identical amounts.
    """
    token_in  = pair_cfg["token_in"]
    token_out = pair_cfg["token_out"]
    dec_in    = pair_cfg["dec_in"]
    dec_out   = pair_cfg["dec_out"]
    unit_size = pair_cfg["unit_size"]
    factory   = dex_cfg["factory"]
    fee_pct   = dex_cfg.get("fee_pct", 0.0002)
    amount_in = int(unit_size * (10 ** dec_in))

    try:
        pair_addr = _get_v2_pair(w3, token_in, token_out, factory, stable=False)
    except Exception:
        return None
    if not pair_addr or pair_addr == _ZERO_ADDRESS:
        return None
    if not _check_liquidity(w3, pair_addr, token_in, token_out, dec_in, dec_out,
                             pair_cfg["min_liquidity_usd"]):
        return None

    try:
        pool = w3.eth.contract(
            address=Web3.to_checksum_address(pair_addr),
            abi=_POOL_V2_ABI,
        )
        amount_out_raw = pool.functions.getAmountOut(
            amount_in,
            Web3.to_checksum_address(token_in),
        ).call()

        if amount_out_raw == 0:
            return None

        amount_in_human  = amount_in / (10 ** dec_in)
        amount_out_human = amount_out_raw / (10 ** dec_out)
        price = amount_out_human / amount_in_human if amount_in_human > 0 else 0.0
        block = w3.eth.block_number

        return PriceQuote(
            venue=dex_cfg["name"],
            pair=pair_cfg["name"],
            price=price,
            fee_pct=fee_pct,
            block=block,
            timestamp=time.time(),
            method="execution",
        )
    except Exception as e:
        logger.debug("v2 getAmountOut failed pair=%s dex=%s: %s",
                     pair_cfg["name"], dex_cfg["name"], e)
        return None


# ── Depth-probe helper ────────────────────────────────────────────────────────

def quote_at_amount(
    w3: Web3,
    pair_cfg: dict,
    dex_cfg: dict,
    amount_in_human: float,
) -> Optional[float]:
    """
    Return execution price (token_out per token_in) at a specific input amount.
    Used by find_max_executable_size to probe pool depth at different trade sizes.

    Internally re-uses the existing quote adapters by substituting unit_size
    with the requested amount — same pool-lookup + quoter path, different amount.
    Returns None if the DEX has no pool or the quoter fails.
    """
    if amount_in_human <= 0:
        return None
    modified_cfg = dict(pair_cfg)
    modified_cfg["unit_size"] = amount_in_human
    try:
        dex_type = dex_cfg.get("type", "")
        if dex_type == "slipstream":
            q = _quote_slipstream(w3, modified_cfg, dex_cfg)
        elif dex_type == "uniswap_v3":
            q = _quote_uniswap_v3(w3, modified_cfg, dex_cfg)
        elif dex_type == "uniswap_v2":
            q = _quote_uniswap_v2(w3, modified_cfg, dex_cfg)
        else:
            return None
        return q.price if q is not None and q.price > 0 else None
    except Exception:
        return None


# ── Multicall3 batch helpers ──────────────────────────────────────────────────

def _populate_pool_cache(w3: Web3) -> None:
    """
    Batch-fetch ALL factory pool addresses into _pool_cache via Multicall3.

    One call per (pair, dex, fee/tick) combination — 225 calls total for the
    15-pair × 5-DEX configuration.  Fits in 3 chunks of 100 = 3 round-trips
    (vs. 225 sequential round-trips previously).

    Called once on the first get_all_prices() invocation.  Results are cached
    for the lifetime of the process; factory pool addresses never change.
    """
    global _pool_cache

    calls: list = []
    call_keys: list = []

    for pair_cfg in config.PAIR_CONFIG:
        token_in  = Web3.to_checksum_address(pair_cfg["token_in"])
        token_out = Web3.to_checksum_address(pair_cfg["token_out"])
        pair_name = pair_cfg["name"]

        for dex_cfg in config.DEX_CONFIG:
            dex_name = dex_cfg["name"]
            dex_type = dex_cfg["type"]
            factory  = Web3.to_checksum_address(dex_cfg["factory"])

            if dex_type == "slipstream":
                for ts in dex_cfg.get("tick_spacings", [1, 50, 100, 200]):
                    key = (pair_name, dex_name, ts)
                    cd  = _SEL_GETPOOL_TICK + abi_encode(
                        ["address", "address", "int24"], [token_in, token_out, ts]
                    )
                    calls.append({"target": factory, "callData": cd})
                    call_keys.append(key)

            elif dex_type == "uniswap_v3":
                for fee in dex_cfg.get("fee_tiers", [500]):
                    key = (pair_name, dex_name, fee)
                    cd  = _SEL_GETPOOL_FEE + abi_encode(
                        ["address", "address", "uint24"], [token_in, token_out, fee]
                    )
                    calls.append({"target": factory, "callData": cd})
                    call_keys.append(key)

            elif dex_type == "uniswap_v2":
                key = (pair_name, dex_name, "v2")
                cd  = _SEL_GETPAIR + abi_encode(
                    ["address", "address", "bool"], [token_in, token_out, False]
                )
                calls.append({"target": factory, "callData": cd})
                call_keys.append(key)

    if not calls:
        return

    logger.info("Pool cache: fetching %d factory addresses via Multicall3", len(calls))

    CHUNK_SIZE = 100
    all_raw: list = []
    for i in range(0, len(calls), CHUNK_SIZE):
        chunk = calls[i : i + CHUNK_SIZE]
        try:
            all_raw.extend(multicall3(w3, chunk))
        except Exception as exc:
            logger.warning(
                "Pool cache batch [%d:%d] failed: %s", i, i + len(chunk), exc
            )
            all_raw.extend([None] * len(chunk))

    valid = 0
    for key, raw in zip(call_keys, all_raw):
        if raw is None:
            _pool_cache[key] = None
            continue
        try:
            addr = abi_decode(["address"], raw)[0]
            pool = addr if addr.lower() != _ZERO_ADDRESS else None
            _pool_cache[key] = pool
            if pool is not None:
                valid += 1
        except Exception:
            _pool_cache[key] = None

    logger.info(
        "Pool cache ready: %d entries, %d valid pools (factory calls: 0 from now on)",
        len(_pool_cache), valid,
    )


def _batch_all_quotes(w3: Web3) -> dict:
    """
    Build and fire one (or a few) Multicall3 aggregate3 calls covering every
    quoter call for every (pair × DEX × fee/tick) with a known pool address.

    Returns the same {pair_name: [PriceQuote, ...]} dict as the old
    sequential get_all_prices() — only pairs with ≥ 2 quotes are included.

    Chunk size 50: one round-trip for up to 50 subcalls (~40–60 live pools
    for this config), so typically 1–2 round-trips total vs. 300–900 before.
    """
    calls: list = []
    call_meta: list = []  # (pair_cfg, dex_cfg, fee_or_tick, call_type)

    try:
        block = w3.eth.block_number
    except Exception:
        block = 0

    for pair_cfg in config.PAIR_CONFIG:
        token_in  = Web3.to_checksum_address(pair_cfg["token_in"])
        token_out = Web3.to_checksum_address(pair_cfg["token_out"])
        amount_in = int(pair_cfg["unit_size"] * (10 ** pair_cfg["dec_in"]))
        pair_name = pair_cfg["name"]

        for dex_cfg in config.DEX_CONFIG:
            dex_name = dex_cfg["name"]
            dex_type = dex_cfg["type"]

            if dex_type == "slipstream":
                quoter = Web3.to_checksum_address(
                    dex_cfg.get("quoter") or config.AERODROME_SLIPSTREAM_QUOTER
                )
                for ts in dex_cfg.get("tick_spacings", [1, 50, 100, 200]):
                    if _pool_cache.get((pair_name, dex_name, ts)) is None:
                        continue
                    cd = _SEL_QUOTE_SLIP + abi_encode(
                        ["(address,address,uint256,int24,uint160)"],
                        [(token_in, token_out, amount_in, ts, 0)],
                    )
                    calls.append({"target": quoter, "callData": cd})
                    call_meta.append((pair_cfg, dex_cfg, ts, "slipstream"))

            elif dex_type == "uniswap_v3":
                quoter_raw = dex_cfg.get("quoter", "")
                if not quoter_raw:
                    continue
                quoter = Web3.to_checksum_address(quoter_raw)
                for fee in dex_cfg.get("fee_tiers", [500]):
                    if _pool_cache.get((pair_name, dex_name, fee)) is None:
                        continue
                    cd = _SEL_QUOTE_V3 + abi_encode(
                        ["(address,address,uint256,uint24,uint160)"],
                        [(token_in, token_out, amount_in, fee, 0)],
                    )
                    calls.append({"target": quoter, "callData": cd})
                    call_meta.append((pair_cfg, dex_cfg, fee, "uniswap_v3"))

            elif dex_type == "uniswap_v2":
                pool_addr = _pool_cache.get((pair_name, dex_name, "v2"))
                if pool_addr is None:
                    continue
                cd = _SEL_AMOUNT_OUT + abi_encode(
                    ["uint256", "address"], [amount_in, token_in]
                )
                calls.append(
                    {"target": Web3.to_checksum_address(pool_addr), "callData": cd}
                )
                call_meta.append((pair_cfg, dex_cfg, "v2", "uniswap_v2"))

    if not calls:
        logger.debug("No quote calls to batch (empty or fully-missed pool cache)")
        return {}

    CHUNK_SIZE = 50
    all_raw: list = []
    n_chunks = 0
    for i in range(0, len(calls), CHUNK_SIZE):
        chunk = calls[i : i + CHUNK_SIZE]
        n_chunks += 1
        try:
            all_raw.extend(multicall3(w3, chunk))
        except Exception as exc:
            logger.error("Quote batch [%d:%d] failed: %s", i, i + len(chunk), exc)
            all_raw.extend([None] * len(chunk))

    # Decode — keep best (highest) price per (pair, dex)
    best_by_pd: dict = {}

    for raw, (pair_cfg, dex_cfg, fee_or_tick, call_type) in zip(all_raw, call_meta):
        if raw is None:
            continue
        pair_name = pair_cfg["name"]
        dex_name  = dex_cfg["name"]
        dec_out   = pair_cfg["dec_out"]
        unit_size = pair_cfg["unit_size"]

        try:
            if call_type in ("slipstream", "uniswap_v3"):
                amount_out_raw = abi_decode(
                    ["uint256", "uint160", "uint32", "uint256"], raw
                )[0]
            else:
                amount_out_raw = abi_decode(["uint256"], raw)[0]

            if amount_out_raw == 0:
                continue

            price = (amount_out_raw / (10 ** dec_out)) / unit_size
            if price <= 0:
                continue

            if call_type == "slipstream":
                fee_pct = _TICK_SPACING_FEE.get(fee_or_tick, dex_cfg.get("fee_pct", 0.0001))
            elif call_type == "uniswap_v3":
                fee_pct = _FEE_TIER_PCT.get(fee_or_tick, fee_or_tick / 1_000_000)
            else:
                fee_pct = dex_cfg.get("fee_pct", 0.0002)

            q = PriceQuote(
                venue=dex_name, pair=pair_name,
                price=price, fee_pct=fee_pct,
                block=block, timestamp=time.time(),
                method="execution",
            )
            pd_key = (pair_name, dex_name)
            if pd_key not in best_by_pd or q.price > best_by_pd[pd_key].price:
                best_by_pd[pd_key] = q

        except Exception as exc:
            logger.debug("Decode failed %s %s: %s", pair_name, dex_name, exc)

    grouped: dict = defaultdict(list)
    for (pair_name, _), q in best_by_pd.items():
        grouped[pair_name].append(q)

    result = {k: v for k, v in grouped.items() if len(v) >= 2}
    logger.info(
        "BATCH_QUOTES | calls=%d batches=%d pairs_returned=%d",
        len(calls), n_chunks, len(result),
    )
    return result


# ── Main price aggregator (legacy per-pair path, used by quote_at_amount) ─────

def _get_quotes_for_pair(w3: Web3, pair_cfg: dict) -> List[PriceQuote]:
    """
    Fetch prices for one pair across all configured DEXes.
    Returns a list of PriceQuote objects (one best quote per DEX that has liquidity).
    """
    quotes: List[PriceQuote] = []

    for dex in config.DEX_CONFIG:
        dex_type = dex["type"]
        try:
            if dex_type == "slipstream":
                q = _quote_slipstream(w3, pair_cfg, dex)
            elif dex_type == "uniswap_v3":
                q = _quote_uniswap_v3(w3, pair_cfg, dex)
            elif dex_type == "uniswap_v2":
                q = _quote_uniswap_v2(w3, pair_cfg, dex)
            else:
                q = None

            if q is not None and q.price > 0:
                quotes.append(q)
        except Exception as e:
            logger.debug("DEX %s failed for pair %s: %s", dex["name"], pair_cfg["name"], e)

    return quotes


def get_all_prices(w3: Web3) -> dict:
    """
    Fetch prices for all pairs in PAIR_CONFIG across all DEXes in DEX_CONFIG.

    Uses Multicall3 batching to collapse the ~300-900 sequential eth_call
    round-trips into 1-4 batches per cycle:

      Startup  (first call only): batch factory.getPool/getPair for all
                                   225 (pair × DEX × fee/tick) combos → 3 chunks.
      Each cycle:                  batch quoter calls for all known pools
                                   (~40-60 calls) → 1-2 chunks.

    Returns:
        {
            "cbBTC/USDC": [PriceQuote(Aerodrome), PriceQuote(Uniswap), ...],
            "weETH/WETH": [...],
            ...
        }

    Only pairs with ≥ 2 quotes (needed for arb comparison) are included.
    All reads use the w3 argument (dRPC). Never touches Alchemy.
    """
    global _pool_cache_initialized

    if not _pool_cache_initialized:
        _populate_pool_cache(w3)
        _pool_cache_initialized = True

    return _batch_all_quotes(w3)
