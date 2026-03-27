"""
Central configuration for the DEX Arbitrage Bot.
All environment variables loaded here. Call validate() before running.
Never imports from the existing morpho_scanner liquidation bot.
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from web3 import Web3

# Always load from the .env next to this file — never from CWD.
_ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=_ENV_PATH, override=False)

# ── RPC Endpoints ─────────────────────────────────────────────────────────────
BASE_RPC_URL: str     = os.getenv("BASE_RPC_URL", "")
DRPC_RPC_URL: str     = os.getenv("DRPC_RPC_URL", "")
ALCHEMY_EXEC_URL: str = os.getenv("ALCHEMY_EXEC_URL", "")

# ── Wallet ────────────────────────────────────────────────────────────────────
PRIVATE_KEY: str = os.getenv("PRIVATE_KEY", "")
WALLET_ADDRESS: str = "0x6F007D1C4F54954d9cdBb5fea81eB5A41FA9f312"

# ── Deployed contract ─────────────────────────────────────────────────────────
ARB_EXECUTOR_ADDRESS: str = os.getenv("ARB_EXECUTOR_ADDRESS", "")

# ── Bot behaviour ─────────────────────────────────────────────────────────────
EXECUTE_MODE: bool           = os.getenv("EXECUTE_MODE", "false").lower() == "true"
DRY_RUN: bool                = os.getenv("DRY_RUN", "true").lower() == "true"
MIN_NET_PROFIT_USD: float    = float(os.getenv("MIN_NET_PROFIT_USD", "10.0"))
# cbBTC floor ~0.06%, 0.065 = small safety margin
MIN_SPREAD_PCT: float        = float(os.getenv("MIN_SPREAD_PCT", "0.065"))
SCAN_INTERVAL_SECONDS: float = float(os.getenv("SCAN_INTERVAL_SECONDS", "2"))
MAX_FLASH_LOAN_USDC: float   = float(os.getenv("MAX_FLASH_LOAN_USDC", "50000"))

# ── Tier thresholds (net spread in %) ─────────────────────────────────────────
TIER_PRIME_PCT: float    = 0.15   # PRIME: execute at max size ($50k)
TIER_GOOD_PCT: float     = 0.10   # GOOD: execute at normal size ($34k)
TIER_MARGINAL_PCT: float = 0.065  # MARGINAL: execute at half size ($17k) = MIN_SPREAD_PCT

# ── Depth discovery ───────────────────────────────────────────────────────────
# Maximum slippage allowed on each leg during depth probing.
# If neither buy nor sell leg stays within this at any test size, the opp is
# classified as DEPTH_REJECTED before simulation.
MAX_SLIPPAGE_PER_LEG: float = float(os.getenv("MAX_SLIPPAGE_PER_LEG", "0.02"))  # 2%

# ── Execution readiness ───────────────────────────────────────────────────────
# True only when ARB_EXECUTOR_ADDRESS is non-empty (contract deployed).
# Used by should_execute() and startup banner. Evaluated at import time.
EXECUTION_READY: bool = bool(os.getenv("ARB_EXECUTOR_ADDRESS", "").strip())

# ── Quote sanity cap ───────────────────────────────────────────────────────────
# Gross spread above this % is physically impossible between two liquid DEXes on
# the same chain and is treated as bad data (stale pool, slippage mismatch,
# decimal error). Rejected before tier assignment.
# Rationale: largest realistic cross-DEX arb is ~2%; 5% gives 2.5x headroom.
MAX_GROSS_SPREAD_PCT: float = float(os.getenv("MAX_GROSS_SPREAD_PCT", "5.0"))

# ── Tier flash loan sizes ─────────────────────────────────────────────────────
FLASH_PRIME_USDC:    float = 50_000.0
FLASH_GOOD_USDC:     float = 34_000.0
FLASH_MARGINAL_USDC: float = 17_000.0

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_DIR: str = os.getenv("LOG_DIR", "logs")

# ── Protocol addresses (Base mainnet) ─────────────────────────────────────────
UNISWAP_SWAP_ROUTER_02: str = Web3.to_checksum_address("0x2626664c2603336E57B271c5C0b26F421741e481")
UNISWAP_QUOTER_V2: str      = Web3.to_checksum_address("0x3d4e44Eb1374240CE5F1B871ab261CD16335B76a")
UNISWAP_FACTORY: str        = Web3.to_checksum_address("0x33128a8fC17869897dcE68Ed026d694621f6FDfD")

AERODROME_ROUTER: str        = Web3.to_checksum_address("0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43")
AERODROME_FACTORY: str       = Web3.to_checksum_address("0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A")
AERODROME_VAMM_FACTORY: str  = Web3.to_checksum_address("0x420DD381b31aEf6683db6B902084cB0FFECe40Da")
# Aerodrome Slipstream CL Quoter — uses tickSpacing instead of fee in quoteExactInputSingle.
# Address verified: Aerodrome CLQuoter on Base mainnet.
AERODROME_SLIPSTREAM_QUOTER: str = Web3.to_checksum_address("0x254cF9E1E6e233aa1AC962CB9B05b2cfeAaE15b0")

MORPHO_ADDRESS: str  = Web3.to_checksum_address("0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb")
BALANCER_VAULT: str  = Web3.to_checksum_address("0xBA12222222228d8Ba445958a75a0704d566BF2C8")

# ── Token addresses (Base mainnet) ────────────────────────────────────────────
USDC_ADDRESS:   str = Web3.to_checksum_address("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913")
CBBTC_ADDRESS:  str = Web3.to_checksum_address("0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf")
WETH_ADDRESS:   str = Web3.to_checksum_address("0x4200000000000000000000000000000000000006")
WEETH_ADDRESS:  str = Web3.to_checksum_address("0x04C0599Ae5A44757c0af6F9eC3b93da8976c150A")
USDBC_ADDRESS:  str = Web3.to_checksum_address("0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA")
DAI_ADDRESS:    str = Web3.to_checksum_address("0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb")
CBETH_ADDRESS:  str = Web3.to_checksum_address("0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22")
WSTETH_ADDRESS: str = Web3.to_checksum_address("0xc1CBa3fCea344f92D9239c08C0568f6F2F0ee452")
AERO_ADDRESS:   str = Web3.to_checksum_address("0x940181a94A35A4569E4529A3CDfB74e38FD98631")
DEGEN_ADDRESS:  str = Web3.to_checksum_address("0x4ed4E862860beD51a9570b96d89aF5E1B0Efefed")
BRETT_ADDRESS:  str = Web3.to_checksum_address("0x532f27101965dd16442E59d40670FaF5eBB142E4")
VIRTUAL_ADDRESS:str = Web3.to_checksum_address("0x0b3e328455c4059EEb9e3f84b5543F74E24e7E1b")
TOSHI_ADDRESS:  str = Web3.to_checksum_address("0xAC1Bd2486aAf3B5C0fc3Fd868558b082a531B2B4")
CBXRP_ADDRESS:  str = Web3.to_checksum_address("0x4B4143fBe6823D0f21882Ba4B53a5E7C11a7B395")
MOG_ADDRESS:    str = Web3.to_checksum_address("0x2Da56AcB9Ea78330f947bD57C54119Debda7AF71")
HIGHER_ADDRESS: str = Web3.to_checksum_address("0x0578d8A44db98B23BF096A382e016e29a5Ce0ffe")

# Token decimals (legacy — kept for backward compat)
USDC_DECIMALS:  int = 6
CBBTC_DECIMALS: int = 8
WETH_DECIMALS:  int = 18
WEETH_DECIMALS: int = 18

# ── Legacy pool addresses (verified via slot0 calls) ──────────────────────────
AERO_CBBTC_USDC_POOL: str = Web3.to_checksum_address("0x4F5905e36ac07eE1F01ffB939aA7f212A58D5CDF")
AERO_WEETH_WETH_POOL: str = Web3.to_checksum_address("0xbD3cd0D9d429b41F0a2e1C026552Bd598294d5E0")

# ── Legacy fee tiers (kept for backward compat) ───────────────────────────────
AERODROME_FEE_CBBTC_USDC: float  = 0.0001
AERODROME_FEE_WEETH_WETH: float  = 0.0001
UNISWAP_FEE_CBBTC_USDC: int      = 500
UNISWAP_FEE_WEETH_WETH: int      = 100
UNISWAP_FEE_PCT_CBBTC_USDC: float = 0.0005
UNISWAP_FEE_PCT_WEETH_WETH: float = 0.0001

# ── Chain ─────────────────────────────────────────────────────────────────────
BASE_CHAIN_ID: int = 8453

# ── Pair config — 15 high-volume Base pairs ───────────────────────────────────
PAIR_CONFIG = [
    {
        "name": "cbBTC/USDC",
        "token_in":  "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf",
        "token_out": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "dec_in": 8, "dec_out": 6,
        "unit_size": 0.1,
        "min_liquidity_usd": 100_000,
    },
    {
        "name": "weETH/WETH",
        "token_in":  "0x04C0599Ae5A44757c0af6F9eC3b93da8976c150A",
        "token_out": "0x4200000000000000000000000000000000000006",
        "dec_in": 18, "dec_out": 18,
        "unit_size": 1.0,
        "min_liquidity_usd": 50_000,
    },
    {
        "name": "cbETH/WETH",
        "token_in":  "0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22",
        "token_out": "0x4200000000000000000000000000000000000006",
        "dec_in": 18, "dec_out": 18,
        "unit_size": 1.0,
        "min_liquidity_usd": 50_000,
    },
    {
        "name": "wstETH/WETH",
        "token_in":  "0xc1CBa3fCea344f92D9239c08C0568f6F2F0ee452",
        "token_out": "0x4200000000000000000000000000000000000006",
        "dec_in": 18, "dec_out": 18,
        "unit_size": 1.0,
        "min_liquidity_usd": 50_000,
    },
    {
        "name": "WETH/USDC",
        "token_in":  "0x4200000000000000000000000000000000000006",
        "token_out": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "dec_in": 18, "dec_out": 6,
        "unit_size": 1.0,
        "min_liquidity_usd": 200_000,
    },
    {
        "name": "USDC/USDbC",
        "token_in":  "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "token_out": "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",
        "dec_in": 6, "dec_out": 6,
        "unit_size": 1000.0,
        "min_liquidity_usd": 100_000,
    },
    {
        "name": "DAI/USDC",
        "token_in":  "0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb",
        "token_out": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "dec_in": 18, "dec_out": 6,
        "unit_size": 1000.0,
        "min_liquidity_usd": 50_000,
    },
    {
        "name": "AERO/WETH",
        "token_in":  "0x940181a94A35A4569E4529A3CDfB74e38FD98631",
        "token_out": "0x4200000000000000000000000000000000000006",
        "dec_in": 18, "dec_out": 18,
        "unit_size": 1000.0,
        "min_liquidity_usd": 50_000,
    },
    {
        "name": "DEGEN/WETH",
        "token_in":  "0x4ed4E862860beD51a9570b96d89aF5E1B0Efefed",
        "token_out": "0x4200000000000000000000000000000000000006",
        "dec_in": 18, "dec_out": 18,
        "unit_size": 100_000.0,
        "min_liquidity_usd": 30_000,
    },
    {
        "name": "BRETT/WETH",
        "token_in":  "0x532f27101965dd16442E59d40670FaF5eBB142E4",
        "token_out": "0x4200000000000000000000000000000000000006",
        "dec_in": 18, "dec_out": 18,
        "unit_size": 100_000.0,
        "min_liquidity_usd": 30_000,
    },
    {
        "name": "VIRTUAL/WETH",
        "token_in":  "0x0b3e328455c4059EEb9e3f84b5543F74E24e7E1b",
        "token_out": "0x4200000000000000000000000000000000000006",
        "dec_in": 18, "dec_out": 18,
        "unit_size": 1000.0,
        "min_liquidity_usd": 30_000,
    },
    {
        "name": "TOSHI/WETH",
        "token_in":  "0xAC1Bd2486aAf3B5C0fc3Fd868558b082a531B2B4",
        "token_out": "0x4200000000000000000000000000000000000006",
        "dec_in": 18, "dec_out": 18,
        "unit_size": 1_000_000.0,
        "min_liquidity_usd": 20_000,
    },
    {
        "name": "cbXRP/USDC",
        "token_in":  "0x4B4143fBe6823D0f21882Ba4B53a5E7C11a7B395",
        "token_out": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "dec_in": 18, "dec_out": 6,
        "unit_size": 1000.0,
        "min_liquidity_usd": 20_000,
    },
    {
        "name": "MOG/WETH",
        "token_in":  "0x2Da56AcB9Ea78330f947bD57C54119Debda7AF71",
        "token_out": "0x4200000000000000000000000000000000000006",
        "dec_in": 18, "dec_out": 18,
        "unit_size": 10_000_000.0,
        "min_liquidity_usd": 20_000,
    },
    {
        "name": "HIGHER/WETH",
        "token_in":  "0x0578d8A44db98B23BF096A382e016e29a5Ce0ffe",
        "token_out": "0x4200000000000000000000000000000000000006",
        "dec_in": 18, "dec_out": 18,
        "unit_size": 100_000.0,
        "min_liquidity_usd": 20_000,
    },
]

# ── DEX config — 5 DEXes on Base ──────────────────────────────────────────────
DEX_CONFIG = [
    {
        "name": "Aerodrome Slipstream",
        "type": "slipstream",          # Uniswap V3 CL fork; uses CLQuoter for execution quotes
        "factory": "0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A",
        "router":  "0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43",
        "quoter":  "0x254cF9E1E6e233aa1AC962CB9B05b2cfeAaE15b0",  # Aerodrome CLQuoter on Base
        "tick_spacings": [1, 50, 100, 200],
        "fee_pct": 0.0001,             # typical for tick_spacing=1
    },
    {
        "name": "Uniswap V3",
        "type": "uniswap_v3",
        "factory": "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",
        "router":  "0x2626664c2603336E57B271c5C0b26F421741e481",
        "quoter":  "0x3d4e44Eb1374240CE5F1B871ab261CD16335B76a",
        "fee_tiers": [100, 500, 3000, 10000],
    },
    {
        "name": "Aerodrome vAMM",      # Uniswap V2 style
        "type": "uniswap_v2",
        "factory": "0x420DD381b31aEf6683db6B902084cB0FFECe40Da",
        "router":  "0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43",
        "fee_pct": 0.0002,
    },
    {
        "name": "BaseSwap",
        "type": "uniswap_v3",
        "factory": "0x38015D05f4fEC8AFe15D7cc0386a126574e8077B",
        "router":  "0x1B8eea9315bE495187D873DA7773a57b96a6d969",
        "quoter":  "0x4fDBD73aD4B1DDde594BF05497C15f76308eFfb9",
        "fee_tiers": [500, 3000],
    },
    {
        "name": "PancakeSwap V3",
        "type": "uniswap_v3",
        "factory": "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865",
        "router":  "0x1b81D678ffb9C0263b24A97847620C99d213eB14",
        "quoter":  "0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997",
        "fee_tiers": [100, 500, 2500, 10000],
    },
]

# ── Flash loan providers (priority order) ─────────────────────────────────────
FLASH_LOAN_PROVIDERS = [
    {
        "name": "Morpho",
        "address": "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb",
        "fee_pct": 0.0,
        "callback": "onMorphoFlashLoan",
        "priority": 1,
    },
    {
        "name": "Balancer",
        "address": "0xBA12222222228d8Ba445958a75a0704d566BF2C8",
        "fee_pct": 0.0,
        "callback": "receiveFlashLoan",
        "priority": 2,
    },
]


def validate() -> None:
    """Raise ValueError if required configuration is missing or invalid."""
    if not BASE_RPC_URL:
        raise ValueError(
            "BASE_RPC_URL is empty — .env not loaded.\n"
            f"  Expected .env at: {_ENV_PATH}"
        )
    if EXECUTE_MODE:
        if not PRIVATE_KEY:
            raise ValueError(
                "PRIVATE_KEY is required when EXECUTE_MODE=true. "
                "Set it in .env (include the 0x prefix)."
            )
        if not ARB_EXECUTOR_ADDRESS:
            raise ValueError(
                "ARB_EXECUTOR_ADDRESS is required when EXECUTE_MODE=true. "
                "Run: python deploy/deploy.py"
            )
