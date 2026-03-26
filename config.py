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
MIN_SPREAD_PCT: float        = float(os.getenv("MIN_SPREAD_PCT", "0.08"))
SCAN_INTERVAL_SECONDS: float = float(os.getenv("SCAN_INTERVAL_SECONDS", "2"))
MAX_FLASH_LOAN_USDC: float   = float(os.getenv("MAX_FLASH_LOAN_USDC", "50000"))

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_DIR: str = os.getenv("LOG_DIR", "logs")

# ── Uniswap V3 (Base mainnet) ─────────────────────────────────────────────────
UNISWAP_SWAP_ROUTER_02: str = Web3.to_checksum_address("0x2626664c2603336E57B271c5C0b26F421741e481")
UNISWAP_QUOTER_V2: str      = Web3.to_checksum_address("0x3d4e44Eb1374240CE5F1B871ab261CD16335B76a")
UNISWAP_FACTORY: str        = Web3.to_checksum_address("0x33128a8fC17869897dcE68Ed026d694621f6FDfD")

# ── Aerodrome (Base mainnet) ──────────────────────────────────────────────────
AERODROME_ROUTER: str  = Web3.to_checksum_address("0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43")
AERODROME_FACTORY: str = Web3.to_checksum_address("0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A")

# ── Morpho (flash loan) ───────────────────────────────────────────────────────
MORPHO_ADDRESS: str = Web3.to_checksum_address("0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb")

# ── Token addresses (Base mainnet) ────────────────────────────────────────────
USDC_ADDRESS:  str = Web3.to_checksum_address("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913")
CBBTC_ADDRESS: str = Web3.to_checksum_address("0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf")
WETH_ADDRESS:  str = Web3.to_checksum_address("0x4200000000000000000000000000000000000006")
WEETH_ADDRESS: str = Web3.to_checksum_address("0x04C0599Ae5A44757c0af6F9eC3b93da8976c150A")

# Token decimals
USDC_DECIMALS:  int = 6
CBBTC_DECIMALS: int = 8
WETH_DECIMALS:  int = 18
WEETH_DECIMALS: int = 18

# ── Pool addresses (verified via slot0 calls) ─────────────────────────────────
AERO_CBBTC_USDC_POOL: str = Web3.to_checksum_address("0x4F5905e36ac07eE1F01ffB939aA7f212A58D5CDF")
AERO_WEETH_WETH_POOL: str = Web3.to_checksum_address("0xbD3cd0D9d429b41F0a2e1C026552Bd598294d5E0")

# ── Fee tiers ─────────────────────────────────────────────────────────────────
AERODROME_FEE_CBBTC_USDC: float = 0.0001   # 0.01% — tick=1
AERODROME_FEE_WEETH_WETH: float = 0.0001   # 0.01%
UNISWAP_FEE_CBBTC_USDC: int     = 500      # 0.05%
UNISWAP_FEE_WEETH_WETH: int     = 100      # 0.01%
UNISWAP_FEE_PCT_CBBTC_USDC: float = 0.0005
UNISWAP_FEE_PCT_WEETH_WETH: float = 0.0001

# ── Chain ─────────────────────────────────────────────────────────────────────
BASE_CHAIN_ID: int = 8453


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
