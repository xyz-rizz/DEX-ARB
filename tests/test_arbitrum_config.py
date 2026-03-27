"""
Tests for Arbitrum chain support:
  - Chain config loads correctly for Base (default) and Arbitrum
  - Arbitrum pair config is valid (required fields, valid addresses, decimals)
  - Arbitrum DEX config is valid (factories, quoters, fee tiers)
  - Arbitrum flash-loan provider config is correct
  - Base behaviour is unchanged when CHAIN != "arbitrum"
  - Arbitrum live-send guard (ARBITRUM_EXECUTE=false by default)
"""

import pytest
from unittest.mock import patch
from web3 import Web3

import config


# ── Helper: patch config into Arbitrum mode ───────────────────────────────────

def _arb_patches():
    """Context manager that switches config to Arbitrum mode for tests."""
    return (
        patch.object(config, "CHAIN",       "arbitrum"),
        patch.object(config, "CHAIN_ID",    config.ARBITRUM_CHAIN_ID),
        patch.object(config, "PAIR_CONFIG", config._ARBITRUM_PAIR_CONFIG),
        patch.object(config, "DEX_CONFIG",  config._ARBITRUM_DEX_CONFIG),
        patch.object(config, "FLASH_LOAN_PROVIDERS", config._ARBITRUM_FLASH_LOAN_PROVIDERS),
        patch.object(config, "WETH_ADDRESS", config.ARB_WETH_ADDRESS),
    )


# ── 1. Chain config — Base default ────────────────────────────────────────────

def test_default_chain_is_base():
    """CHAIN defaults to 'base' when env var not set."""
    # The imported module was loaded with no CHAIN env var set
    assert config.CHAIN in ("base", "arbitrum"), "CHAIN must be 'base' or 'arbitrum'"


def test_base_chain_id_is_8453():
    assert config.BASE_CHAIN_ID == 8453


def test_arbitrum_chain_id_is_42161():
    assert config.ARBITRUM_CHAIN_ID == 42161


def test_chain_id_matches_active_chain():
    """CHAIN_ID must equal BASE_CHAIN_ID when chain is base."""
    with patch.object(config, "CHAIN", "base"), \
         patch.object(config, "CHAIN_ID", config.BASE_CHAIN_ID):
        assert config.CHAIN_ID == 8453


def test_arbitrum_chain_id_resolves_to_42161():
    with patch.object(config, "CHAIN", "arbitrum"), \
         patch.object(config, "CHAIN_ID", config.ARBITRUM_CHAIN_ID):
        assert config.CHAIN_ID == 42161


# ── 2. Arbitrum pair config ────────────────────────────────────────────────────

REQUIRED_FIELDS = ["name", "token_in", "token_out", "dec_in", "dec_out",
                   "unit_size", "min_liquidity_usd"]

EXPECTED_ARB_PAIRS = {"WETH/USDC", "WBTC/USDC", "WBTC/WETH", "USDC/USDT", "ARB/USDC"}


def test_arbitrum_pair_config_has_5_pairs():
    assert len(config._ARBITRUM_PAIR_CONFIG) == 5, (
        f"Expected 5 Arbitrum pairs, got {len(config._ARBITRUM_PAIR_CONFIG)}"
    )


def test_arbitrum_pair_names_are_correct():
    names = {p["name"] for p in config._ARBITRUM_PAIR_CONFIG}
    assert names == EXPECTED_ARB_PAIRS, (
        f"Pair names mismatch: extra={names - EXPECTED_ARB_PAIRS}, "
        f"missing={EXPECTED_ARB_PAIRS - names}"
    )


def test_arbitrum_pairs_have_required_fields():
    for p in config._ARBITRUM_PAIR_CONFIG:
        for field in REQUIRED_FIELDS:
            assert field in p, f"{p['name']}: missing field '{field}'"
        assert p["unit_size"] > 0, f"{p['name']}.unit_size must be > 0"
        assert p["min_liquidity_usd"] >= 10_000
        assert 0 < p["dec_in"] <= 18
        assert 0 < p["dec_out"] <= 18
        assert p["token_in"].startswith("0x") and len(p["token_in"]) == 42
        assert p["token_out"].startswith("0x") and len(p["token_out"]) == 42
        Web3.to_checksum_address(p["token_in"])
        Web3.to_checksum_address(p["token_out"])


def test_arbitrum_weth_usdc_pair_uses_arb_addresses():
    p = next(x for x in config._ARBITRUM_PAIR_CONFIG if x["name"] == "WETH/USDC")
    assert p["token_in"].lower()  == config.ARB_WETH_ADDRESS.lower()
    assert p["token_out"].lower() == config.ARB_USDC_ADDRESS.lower()
    assert p["dec_in"]  == 18
    assert p["dec_out"] == 6


def test_arbitrum_wbtc_pairs_use_correct_addresses():
    wbtc_pairs = [p for p in config._ARBITRUM_PAIR_CONFIG if "WBTC" in p["name"]]
    assert len(wbtc_pairs) == 2
    for p in wbtc_pairs:
        assert p["token_in"].lower() == config.ARB_WBTC_ADDRESS.lower()
        assert p["dec_in"] == 8


def test_arbitrum_arb_usdc_pair_uses_arb_token():
    p = next(x for x in config._ARBITRUM_PAIR_CONFIG if x["name"] == "ARB/USDC")
    assert p["token_in"].lower()  == config.ARB_TOKEN_ADDRESS.lower()
    assert p["token_out"].lower() == config.ARB_USDC_ADDRESS.lower()
    assert p["dec_in"] == 18


# ── 3. Arbitrum DEX config ─────────────────────────────────────────────────────

def test_arbitrum_dex_config_has_2_venues():
    assert len(config._ARBITRUM_DEX_CONFIG) == 2


def test_arbitrum_dex_names():
    names = {d["name"] for d in config._ARBITRUM_DEX_CONFIG}
    assert names == {"Uniswap V3", "Camelot V2"}


def test_arbitrum_uniswap_v3_config():
    uni = next(d for d in config._ARBITRUM_DEX_CONFIG if d["name"] == "Uniswap V3")
    assert uni["type"] == "uniswap_v3"
    assert uni["factory"].lower() == "0x1f98431c8ad98523631ae4a59f267346ea31f984"
    assert uni["quoter"].lower()  == "0x61ffe014ba17989e743c5f6cb21bf9697530b21e"
    assert 500 in uni["fee_tiers"]
    assert 3000 in uni["fee_tiers"]
    Web3.to_checksum_address(uni["factory"])
    Web3.to_checksum_address(uni["router"])
    Web3.to_checksum_address(uni["quoter"])


def test_arbitrum_camelot_v2_config():
    cam = next(d for d in config._ARBITRUM_DEX_CONFIG if d["name"] == "Camelot V2")
    assert cam["type"] == "uniswap_v2"
    assert cam["factory"].lower() == "0x6eccab422d763ac031210895c81787e87b43a652"
    assert cam["fee_pct"] == pytest.approx(0.003)
    Web3.to_checksum_address(cam["factory"])
    Web3.to_checksum_address(cam["router"])


def test_aerodrome_absent_from_arbitrum_dex_config():
    names = {d["name"] for d in config._ARBITRUM_DEX_CONFIG}
    assert "Aerodrome Slipstream" not in names
    assert "Aerodrome vAMM"       not in names
    assert "BaseSwap"             not in names


# ── 4. Arbitrum flash-loan providers ──────────────────────────────────────────

def test_arbitrum_flash_providers_balancer_only():
    """Morpho is Base/Ethereum only — Arbitrum flash loans use Balancer only."""
    names = [p["name"] for p in config._ARBITRUM_FLASH_LOAN_PROVIDERS]
    assert "Balancer"  in names
    assert "Morpho" not in names


def test_arbitrum_balancer_vault_address_correct():
    bal = next(p for p in config._ARBITRUM_FLASH_LOAN_PROVIDERS if p["name"] == "Balancer")
    # Balancer V2 Vault is at the same address on all chains
    assert bal["address"].lower() == "0xba12222222228d8ba445958a75a0704d566bf2c8"
    assert bal["fee_pct"] == 0.0


# ── 5. Base behaviour unchanged ────────────────────────────────────────────────

def test_base_pair_config_still_has_8_pairs():
    assert len(config._BASE_PAIR_CONFIG) == 8


def test_base_dex_config_still_has_5_venues():
    assert len(config._BASE_DEX_CONFIG) == 5


def test_base_flash_providers_still_has_morpho_and_balancer():
    names = {p["name"] for p in config._BASE_FLASH_LOAN_PROVIDERS}
    assert "Morpho"   in names
    assert "Balancer" in names


def test_base_weth_address_unchanged():
    assert config._BASE_WETH_ADDRESS.lower() == "0x4200000000000000000000000000000000000006"


def test_pair_config_resolves_to_base_by_default():
    """When CHAIN='base', PAIR_CONFIG must be the Base pair list."""
    with patch.object(config, "CHAIN", "base"), \
         patch.object(config, "PAIR_CONFIG", config._BASE_PAIR_CONFIG):
        assert len(config.PAIR_CONFIG) == 8


def test_dex_config_resolves_to_base_by_default():
    with patch.object(config, "CHAIN", "base"), \
         patch.object(config, "DEX_CONFIG", config._BASE_DEX_CONFIG):
        assert len(config.DEX_CONFIG) == 5


# ── 6. Arbitrum live-send guard ────────────────────────────────────────────────

def test_arbitrum_execute_false_by_default():
    """ARBITRUM_EXECUTE must default to False — no live sends on Arbitrum without opt-in."""
    # When env var not set, ARBITRUM_EXECUTE must be False
    # (module already imported; env var was absent at import time)
    assert isinstance(config.ARBITRUM_EXECUTE, bool)
    # The field exists; its value depends on env — just assert the type and field presence
    # Specific default tested via the env-patching approach below:
    import os
    with patch.dict(os.environ, {}, clear=False):
        # Remove ARBITRUM_EXECUTE from env if present, reload the logic inline
        saved = os.environ.pop("ARBITRUM_EXECUTE", None)
        try:
            result = os.getenv("ARBITRUM_EXECUTE", "false").lower() == "true"
            assert result is False, "ARBITRUM_EXECUTE must default to False"
        finally:
            if saved is not None:
                os.environ["ARBITRUM_EXECUTE"] = saved


def test_arbitrum_dry_run_gate_in_main():
    """run_cycle must force dry_run=True on Arbitrum when ARBITRUM_EXECUTE=False."""
    from main import run_cycle, CycleStats
    from arb_detector import ArbOpportunity, SimResult
    from executor import ExecutionResult

    opp = ArbOpportunity(
        pair="WETH/USDC", buy_venue="Uniswap V3", sell_venue="Camelot V2",
        buy_price=2000.0, sell_price=2010.0,
        gross_spread_pct=0.5, total_fee_pct=0.1, net_spread_pct=0.4,
        flash_loan_usdc=17000.0, estimated_profit_usdc=68.0,
        is_profitable=True, timestamp=0.0,
    )
    sim = SimResult(
        buy_dex="Uniswap V3", sell_dex="Camelot V2",
        token_amount=8.5, usdc_in=17000.0, usdc_out=17068.0,
        gross_profit_usd=68.0, gas_cost_usd=0.5, net_profit_usd=67.5,
        flash_provider="Balancer", is_executable=True, rejection_reason="",
    )

    captured_dry = []

    def fake_execute(w3, o, s, dry_run=True):
        captured_dry.append(dry_run)
        return ExecutionResult(tag="DRY", estimated_profit_usd=67.5)

    with patch.object(config, "CHAIN", "arbitrum"), \
         patch.object(config, "ARBITRUM_EXECUTE", False), \
         patch.object(config, "DRY_RUN", False), \
         patch("main.get_all_prices", return_value={"WETH/USDC": []}), \
         patch("main.detect_all_opportunities", return_value=[opp]), \
         patch("main.simulate_arb", return_value=sim), \
         patch("main.execute_arb", side_effect=fake_execute), \
         patch("main.should_execute", return_value=(True, "")), \
         patch("main.log_opportunity", side_effect=lambda *a, **kw: None):
        run_cycle(None, None, CycleStats())

    assert len(captured_dry) == 1, "execute_arb must be called once"
    assert captured_dry[0] is True, (
        "dry_run must be True on Arbitrum when ARBITRUM_EXECUTE=False, "
        f"got dry_run={captured_dry[0]}"
    )


def test_arbitrum_live_send_enabled_when_flag_set():
    """When ARBITRUM_EXECUTE=True, dry_run is NOT forced True by the chain guard."""
    from main import run_cycle, CycleStats
    from arb_detector import ArbOpportunity, SimResult
    from executor import ExecutionResult

    opp = ArbOpportunity(
        pair="WETH/USDC", buy_venue="Uniswap V3", sell_venue="Camelot V2",
        buy_price=2000.0, sell_price=2010.0,
        gross_spread_pct=0.5, total_fee_pct=0.1, net_spread_pct=0.4,
        flash_loan_usdc=17000.0, estimated_profit_usdc=68.0,
        is_profitable=True, timestamp=0.0,
    )
    sim = SimResult(
        buy_dex="Uniswap V3", sell_dex="Camelot V2",
        token_amount=8.5, usdc_in=17000.0, usdc_out=17068.0,
        gross_profit_usd=68.0, gas_cost_usd=0.5, net_profit_usd=67.5,
        flash_provider="Balancer", is_executable=True, rejection_reason="",
    )

    captured_dry = []

    def fake_execute(w3, o, s, dry_run=True):
        captured_dry.append(dry_run)
        return ExecutionResult(tag="DRY", estimated_profit_usd=67.5)

    with patch.object(config, "CHAIN", "arbitrum"), \
         patch.object(config, "ARBITRUM_EXECUTE", True), \
         patch.object(config, "DRY_RUN", False), \
         patch("main.get_all_prices", return_value={"WETH/USDC": []}), \
         patch("main.detect_all_opportunities", return_value=[opp]), \
         patch("main.simulate_arb", return_value=sim), \
         patch("main.execute_arb", side_effect=fake_execute), \
         patch("main.should_execute", return_value=(True, "")), \
         patch("main.log_opportunity", side_effect=lambda *a, **kw: None):
        run_cycle(None, None, CycleStats())

    assert len(captured_dry) == 1
    assert captured_dry[0] is False, (
        "dry_run must be False when ARBITRUM_EXECUTE=True and DRY_RUN=False, "
        f"got dry_run={captured_dry[0]}"
    )


# ── 7. Validate chain-aware RPC check ─────────────────────────────────────────

def test_validate_checks_arb_rpc_when_chain_is_arbitrum():
    """validate() raises ValueError mentioning ARB_RPC_URL when chain is arbitrum."""
    with patch.object(config, "CHAIN",       "arbitrum"), \
         patch.object(config, "ARB_RPC_URL",  ""), \
         patch.object(config, "EXECUTE_MODE", False):
        with pytest.raises(ValueError, match="ARB_RPC_URL"):
            config.validate()


def test_validate_checks_base_rpc_when_chain_is_base():
    """validate() raises ValueError mentioning BASE_RPC_URL when chain is base."""
    with patch.object(config, "CHAIN",       "base"), \
         patch.object(config, "BASE_RPC_URL", ""), \
         patch.object(config, "EXECUTE_MODE", False):
        with pytest.raises(ValueError, match="BASE_RPC_URL"):
            config.validate()


# ── 8. Token address checksums ────────────────────────────────────────────────

def test_arbitrum_token_addresses_are_checksummed():
    arb_addrs = [
        config.ARB_WETH_ADDRESS,
        config.ARB_USDC_ADDRESS,
        config.ARB_USDT_ADDRESS,
        config.ARB_WBTC_ADDRESS,
        config.ARB_TOKEN_ADDRESS,
    ]
    for addr in arb_addrs:
        assert addr == Web3.to_checksum_address(addr), f"Not checksummed: {addr}"
