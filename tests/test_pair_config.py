"""
Tests for PAIR_CONFIG update:
  - Exactly 8 pairs
  - Dead pairs removed
  - New pairs have required fields and valid addresses
  - Kept pairs unchanged
"""

import pytest
from web3 import Web3


EXPECTED_PAIRS = {
    "cbBTC/USDC",
    "cbBTC/WETH",
    "WETH/USDC",
    "VIRTUAL/WETH",
    "BRETT/WETH",
    "EURC/USDC",
    "USDC/USDT",
    "AERO/USDC",
}

REMOVED_PAIRS = [
    "weETH/WETH",
    "cbETH/WETH",
    "wstETH/WETH",
    "USDC/USDbC",
    "DAI/USDC",
    "DEGEN/WETH",
    "TOSHI/WETH",
    "cbXRP/USDC",
    "MOG/WETH",
    "HIGHER/WETH",
    "AERO/WETH",
]

NEW_PAIRS = ["cbBTC/WETH", "EURC/USDC", "USDC/USDT", "AERO/USDC"]

REQUIRED_FIELDS = ["name", "token_in", "token_out", "dec_in", "dec_out",
                   "unit_size", "min_liquidity_usd"]


def _pair(name: str) -> dict:
    from config import PAIR_CONFIG
    return next(p for p in PAIR_CONFIG if p["name"] == name)


# ── Test 1 ─────────────────────────────────────────────────────────────────────

def test_pair_config_has_exactly_8_pairs():
    """PAIR_CONFIG must contain exactly 8 pairs with the expected names."""
    from config import PAIR_CONFIG
    assert len(PAIR_CONFIG) == 8, (
        f"Expected 8 pairs, got {len(PAIR_CONFIG)}: {[p['name'] for p in PAIR_CONFIG]}"
    )
    names = {p["name"] for p in PAIR_CONFIG}
    assert names == EXPECTED_PAIRS, (
        f"Pair names mismatch.\n"
        f"  Extra:   {names - EXPECTED_PAIRS}\n"
        f"  Missing: {EXPECTED_PAIRS - names}"
    )


# ── Test 2 ─────────────────────────────────────────────────────────────────────

def test_removed_pairs_not_in_config():
    """All 11 dead pairs must be absent from PAIR_CONFIG."""
    from config import PAIR_CONFIG
    names = {p["name"] for p in PAIR_CONFIG}
    still_present = [r for r in REMOVED_PAIRS if r in names]
    assert still_present == [], (
        f"Dead pairs still in config: {still_present}"
    )


# ── Test 3 ─────────────────────────────────────────────────────────────────────

def test_new_pairs_have_required_fields():
    """Each new pair must have all required fields, valid addresses, unit_size > 0, min_liquidity >= 10000."""
    for pair_name in NEW_PAIRS:
        p = _pair(pair_name)

        # All required fields present
        for field in REQUIRED_FIELDS:
            assert field in p, f"{pair_name}: missing field '{field}'"

        # token addresses are valid 42-char hex strings
        for addr_field in ("token_in", "token_out"):
            addr = p[addr_field]
            assert isinstance(addr, str), f"{pair_name}.{addr_field} must be str"
            assert len(addr) == 42, f"{pair_name}.{addr_field} wrong length: {addr}"
            assert addr.startswith("0x"), f"{pair_name}.{addr_field} missing 0x: {addr}"
            # Round-trip through Web3 checksum — raises ValueError if invalid
            Web3.to_checksum_address(addr)

        # unit_size > 0
        assert p["unit_size"] > 0, f"{pair_name}.unit_size must be > 0"

        # min_liquidity_usd >= 10000
        assert p["min_liquidity_usd"] >= 10_000, (
            f"{pair_name}.min_liquidity_usd must be >= 10000, got {p['min_liquidity_usd']}"
        )

        # dec_in and dec_out are reasonable decimals
        assert 0 < p["dec_in"] <= 18, f"{pair_name}.dec_in out of range: {p['dec_in']}"
        assert 0 < p["dec_out"] <= 18, f"{pair_name}.dec_out out of range: {p['dec_out']}"


# ── Test 4 ─────────────────────────────────────────────────────────────────────

def test_existing_pairs_unchanged():
    """The 4 kept pairs must be unchanged from their original values."""
    # cbBTC/USDC
    cbbtc_usdc = _pair("cbBTC/USDC")
    assert cbbtc_usdc["token_in"].lower() == "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"
    assert cbbtc_usdc["token_out"].lower() == "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
    assert cbbtc_usdc["dec_in"] == 8
    assert cbbtc_usdc["dec_out"] == 6
    assert cbbtc_usdc["unit_size"] == 0.1
    assert cbbtc_usdc["min_liquidity_usd"] == 100_000

    # WETH/USDC
    weth_usdc = _pair("WETH/USDC")
    assert weth_usdc["token_in"].lower() == "0x4200000000000000000000000000000000000006"
    assert weth_usdc["token_out"].lower() == "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
    assert weth_usdc["dec_in"] == 18
    assert weth_usdc["dec_out"] == 6
    assert weth_usdc["unit_size"] == 1.0
    assert weth_usdc["min_liquidity_usd"] == 200_000

    # BRETT/WETH — still present
    brett = _pair("BRETT/WETH")
    assert brett["token_in"].lower() == "0x532f27101965dd16442e59d40670faf5ebb142e4"
    assert brett["dec_in"] == 18
    assert brett["dec_out"] == 18

    # VIRTUAL/WETH — still present
    virtual = _pair("VIRTUAL/WETH")
    assert virtual["token_in"].lower() == "0x0b3e328455c4059eeb9e3f84b5543f74e24e7e1b"
    assert virtual["dec_in"] == 18
    assert virtual["dec_out"] == 18
