"""
Compile and deploy ArbExecutor.sol to Base mainnet.

Prerequisites:
  - pip install -r requirements.txt   (py-solc-x included)
  - PRIVATE_KEY set in .env
  - BASE_RPC_URL or ALCHEMY_EXEC_URL set in .env (Alchemy preferred for writes)

Usage:
  cd /Users/rizz/DEX\ Arb
  python deploy/deploy.py

On success:
  - Prints: "ArbExecutor deployed at: 0x..."
  - Writes ARB_EXECUTOR_ADDRESS=0x... to .env
  - Verifies: calls owner() — must return your wallet address

This script never imports from the morpho_scanner liquidation bot.
"""

import json
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv, set_key
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware

_log = logging.getLogger(__name__)

# ── Paths (resolved relative to this file's parent) ───────────────────────────
_ROOT     = Path(__file__).resolve().parent.parent
_SOL_PATH = _ROOT / "contracts" / "ArbExecutor.sol"
_ABI_CACHE = _ROOT / "contracts" / "ArbExecutor.abi.json"
_BIN_CACHE = _ROOT / "contracts" / "ArbExecutor.bin"
_ENV_PATH  = _ROOT / ".env"

_SOLC_VERSION = "0.8.19"

# Ensure project root importable when run directly
sys.path.insert(0, str(_ROOT))
load_dotenv(dotenv_path=_ENV_PATH, override=False)

import config  # noqa: E402 — after sys.path


# ── Compilation ───────────────────────────────────────────────────────────────

def compile_contract() -> tuple:
    """
    Compile ArbExecutor.sol with solc 0.8.19.
    Caches ABI and bytecode in contracts/ to avoid re-compilation.
    Returns (abi: list, bytecode: str).
    """
    # Return cached artefacts if .sol hasn't changed
    if (
        _ABI_CACHE.exists()
        and _BIN_CACHE.exists()
        and _ABI_CACHE.stat().st_mtime >= _SOL_PATH.stat().st_mtime
    ):
        abi      = json.loads(_ABI_CACHE.read_text())
        bytecode = _BIN_CACHE.read_text().strip()
        _log.debug("[deploy] Using cached artefacts")
        return abi, bytecode

    print(f"[deploy] Compiling {_SOL_PATH} with solc {_SOLC_VERSION}…")
    try:
        import solcx
    except ImportError:
        print("[deploy] ERROR: py-solc-x not installed. Run: pip install py-solc-x==1.1.1")
        sys.exit(1)

    try:
        solcx.install_solc(_SOLC_VERSION, show_progress=True)
    except Exception as exc:
        print(f"[deploy] WARNING: install_solc failed ({exc}) — trying existing binary")

    solcx.set_solc_version(_SOLC_VERSION)

    output = solcx.compile_files(
        [str(_SOL_PATH)],
        output_values=["abi", "bin"],
        solc_version=_SOLC_VERSION,
        optimize=True,
        optimize_runs=200,
    )

    # Find the ArbExecutor contract in output
    contract_key = None
    for key in output:
        if "ArbExecutor" in key:
            contract_key = key
            break

    if contract_key is None:
        print(f"[deploy] ERROR: ArbExecutor not found in compiled output. Keys: {list(output.keys())}")
        sys.exit(1)

    abi      = output[contract_key]["abi"]
    bytecode = output[contract_key]["bin"]

    # Cache
    _ABI_CACHE.write_text(json.dumps(abi, indent=2))
    _BIN_CACHE.write_text(bytecode)
    print(f"[deploy] Compiled OK — ABI cached at {_ABI_CACHE}")

    return abi, bytecode


# ── Deployment ─────────────────────────────────────────────────────────────────

def deploy_contract(abi: list, bytecode: str) -> str:
    """
    Deploy ArbExecutor to Base mainnet.
    Uses ALCHEMY_EXEC_URL if set, otherwise BASE_RPC_URL.
    Returns the deployed contract address (checksummed).
    """
    rpc_url = config.ALCHEMY_EXEC_URL or config.BASE_RPC_URL
    if not rpc_url:
        print("[deploy] ERROR: Neither ALCHEMY_EXEC_URL nor BASE_RPC_URL is set in .env")
        sys.exit(1)

    if not config.PRIVATE_KEY:
        print("[deploy] ERROR: PRIVATE_KEY not set in .env")
        sys.exit(1)

    w3 = Web3(Web3.HTTPProvider(rpc_url))
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

    if not w3.is_connected():
        print(f"[deploy] ERROR: Cannot connect to RPC: {rpc_url}")
        sys.exit(1)

    account = w3.eth.account.from_key(config.PRIVATE_KEY)
    deployer = account.address
    balance  = w3.eth.get_balance(deployer)
    balance_eth = w3.from_wei(balance, "ether")

    print(f"[deploy] Deployer:  {deployer}")
    print(f"[deploy] Balance:   {balance_eth:.6f} ETH")
    print(f"[deploy] Chain ID:  {w3.eth.chain_id}")
    print(f"[deploy] Block:     {w3.eth.block_number}")

    if balance == 0:
        print("[deploy] ERROR: Deployer wallet has zero ETH — fund it first")
        sys.exit(1)

    Contract = w3.eth.contract(abi=abi, bytecode=bytecode)

    # Build deploy transaction
    nonce    = w3.eth.get_transaction_count(deployer)
    gas_price = w3.eth.gas_price

    tx = Contract.constructor().build_transaction({
        "from":     deployer,
        "nonce":    nonce,
        "gasPrice": gas_price,
        "chainId":  config.BASE_CHAIN_ID,
    })

    # Estimate gas with 20% buffer
    estimated = w3.eth.estimate_gas(tx)
    tx["gas"] = int(estimated * 1.2)

    print(f"[deploy] Gas estimate: {tx['gas']:,}")
    print(f"[deploy] Signing and broadcasting…")

    signed = account.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    print(f"[deploy] Tx broadcast: {tx_hash.hex()}")

    print("[deploy] Waiting for confirmation…")
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)

    if receipt.status != 1:
        print(f"[deploy] ERROR: Deployment reverted. Receipt: {receipt}")
        sys.exit(1)

    contract_address = receipt.contractAddress
    print(f"[deploy] ArbExecutor deployed at: {contract_address}")

    return contract_address


# ── Post-deploy verification ───────────────────────────────────────────────────

def verify_deployment(abi: list, address: str) -> None:
    """Call owner() on deployed contract and verify it matches wallet address."""
    rpc_url = config.ALCHEMY_EXEC_URL or config.BASE_RPC_URL
    w3 = Web3(Web3.HTTPProvider(rpc_url))

    contract = w3.eth.contract(
        address=Web3.to_checksum_address(address),
        abi=abi,
    )
    owner = contract.functions.owner().call()
    expected = Web3.to_checksum_address(config.WALLET_ADDRESS)

    if Web3.to_checksum_address(owner) != expected:
        print(f"[deploy] WARNING: owner() returned {owner}, expected {expected}")
    else:
        print(f"[deploy] Verified: owner() == {owner} ✅")


# ── Write address back to .env ─────────────────────────────────────────────────

def write_address_to_env(address: str) -> None:
    """Write ARB_EXECUTOR_ADDRESS=<address> to .env."""
    set_key(str(_ENV_PATH), "ARB_EXECUTOR_ADDRESS", address)
    print(f"[deploy] ARB_EXECUTOR_ADDRESS written to {_ENV_PATH}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    print("[deploy] Starting ArbExecutor deployment…")
    abi, bytecode = compile_contract()
    address = deploy_contract(abi, bytecode)
    verify_deployment(abi, address)
    write_address_to_env(address)

    print(f"\n[deploy] Done.\n  Contract: {address}\n  .env updated with ARB_EXECUTOR_ADDRESS")


if __name__ == "__main__":
    main()
