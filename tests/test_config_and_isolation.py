"""
Tests for config validation, address checksums, isolation, and main loop resilience.
"""

import json
import subprocess
import time
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

import config
from arb_detector import ArbOpportunity


# ── Config validation ──────────────────────────────────────────────────────────

def test_config_validates_on_missing_base_rpc():
    """validate() must raise ValueError if BASE_RPC_URL is empty."""
    with patch.object(config, "BASE_RPC_URL", ""), \
         patch.object(config, "EXECUTE_MODE", False):
        with pytest.raises(ValueError, match="BASE_RPC_URL"):
            config.validate()


def test_config_validates_execute_mode_missing_private_key():
    """validate() raises ValueError if EXECUTE_MODE=true and PRIVATE_KEY empty."""
    with patch.object(config, "BASE_RPC_URL", "https://rpc.example.com"), \
         patch.object(config, "EXECUTE_MODE", True), \
         patch.object(config, "PRIVATE_KEY", ""), \
         patch.object(config, "ARB_EXECUTOR_ADDRESS", "0xDEAD"):
        with pytest.raises(ValueError, match="PRIVATE_KEY"):
            config.validate()


def test_config_validates_execute_mode_missing_contract():
    """validate() raises ValueError if EXECUTE_MODE=true and ARB_EXECUTOR_ADDRESS empty."""
    with patch.object(config, "BASE_RPC_URL", "https://rpc.example.com"), \
         patch.object(config, "EXECUTE_MODE", True), \
         patch.object(config, "PRIVATE_KEY", "0xabcd"), \
         patch.object(config, "ARB_EXECUTOR_ADDRESS", ""):
        with pytest.raises(ValueError, match="ARB_EXECUTOR_ADDRESS"):
            config.validate()


def test_config_validates_ok_scan_mode():
    """validate() must not raise when EXECUTE_MODE=false and BASE_RPC_URL set."""
    with patch.object(config, "BASE_RPC_URL", "https://rpc.example.com"), \
         patch.object(config, "EXECUTE_MODE", False):
        config.validate()  # should not raise


# ── Address checksums ──────────────────────────────────────────────────────────

def test_config_all_token_addresses_checksummed():
    """All token address constants must be EIP-55 checksummed."""
    from web3 import Web3
    addresses = [
        config.USDC_ADDRESS, config.CBBTC_ADDRESS,
        config.WETH_ADDRESS, config.WEETH_ADDRESS,
    ]
    for addr in addresses:
        assert addr == Web3.to_checksum_address(addr), f"Not checksummed: {addr}"


def test_config_all_contract_addresses_checksummed():
    """All contract address constants must be EIP-55 checksummed."""
    from web3 import Web3
    addresses = [
        config.UNISWAP_SWAP_ROUTER_02, config.UNISWAP_QUOTER_V2,
        config.UNISWAP_FACTORY, config.AERODROME_ROUTER,
        config.AERODROME_FACTORY, config.MORPHO_ADDRESS,
    ]
    for addr in addresses:
        assert addr == Web3.to_checksum_address(addr), f"Not checksummed: {addr}"


def test_config_pool_addresses_checksummed():
    """Pool address constants must be EIP-55 checksummed."""
    from web3 import Web3
    pools = [config.AERO_CBBTC_USDC_POOL, config.AERO_WEETH_WETH_POOL]
    for addr in pools:
        assert addr == Web3.to_checksum_address(addr), f"Not checksummed: {addr}"


# ── Isolation: no imports from existing bot ───────────────────────────────────

def test_no_import_from_existing_bot():
    """
    No Python file in the new project may import from morpho_scanner.
    Checks for actual import statements only — comments are allowed.
    This guarantees complete runtime isolation from the liquidation bot.
    """
    import ast
    project_root = Path(__file__).resolve().parent.parent
    py_files = list(project_root.rglob("*.py"))
    py_files = [f for f in py_files if "venv" not in str(f)]

    violations = []
    for f in py_files:
        try:
            tree = ast.parse(f.read_text(encoding="utf-8", errors="ignore"))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                module = ""
                if isinstance(node, ast.Import):
                    module = ",".join(alias.name for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module:
                    module = node.module
                if "morpho_scanner" in module:
                    violations.append(f"{f}:{node.lineno} → {module}")

    assert violations == [], f"Import from morpho_scanner found: {violations}"


def test_no_arbbot_import_in_project():
    """
    No Python file may import from arbbot package.
    Checks import statements only, not comments or strings.
    """
    import ast
    project_root = Path(__file__).resolve().parent.parent
    py_files = list(project_root.rglob("*.py"))
    py_files = [f for f in py_files if "venv" not in str(f)]

    violations = []
    for f in py_files:
        try:
            tree = ast.parse(f.read_text(encoding="utf-8", errors="ignore"))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                module = ""
                if isinstance(node, ast.Import):
                    module = ",".join(alias.name for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module:
                    module = node.module
                if "arbbot" in module:
                    violations.append(f"{f}:{node.lineno} → {module}")

    assert violations == [], f"Import from arbbot found: {violations}"


# ── Main loop resilience ───────────────────────────────────────────────────────

def test_main_loop_does_not_crash_on_price_error():
    """
    run_cycle must catch price fetch exceptions and continue — never crash.
    """
    import main as main_module
    from main import CycleStats

    w3_mock = MagicMock()
    stats = CycleStats()

    # Simulate price fetch failure
    with patch("main.get_all_prices", side_effect=Exception("RPC timeout")), \
         patch("main.config.LOG_DIR", "/tmp/dex_arb_test_logs"):
        # Should not raise
        main_module.run_cycle(w3_mock, stats)

    assert stats.cycles == 0  # Nothing recorded since prices failed


def test_main_loop_does_not_crash_on_detect_error():
    """run_cycle must catch detect_all_opportunities exceptions."""
    import main as main_module
    from price_scanner import PriceQuote

    mock_prices = {
        "cbBTC/USDC": [
            PriceQuote("aerodrome", "cbBTC/USDC", 68297.0, 0.0001, 1, time.time()),
            PriceQuote("uniswap",   "cbBTC/USDC", 68193.0, 0.0005, 1, time.time()),
        ]
    }
    w3_mock = MagicMock()

    with patch("main.get_all_prices", return_value=mock_prices), \
         patch("main.detect_all_opportunities", side_effect=Exception("detector crash")), \
         patch("main.config.LOG_DIR", "/tmp/dex_arb_test_logs"):
        main_module.run_cycle(w3_mock)  # must not raise


def test_main_loop_logs_profitable_opportunity(tmp_path):
    """run_cycle must call log_opportunity when a profitable opp is detected."""
    import main as main_module
    from price_scanner import PriceQuote
    from arb_detector import ArbOpportunity, SimResult

    mock_prices = {
        "cbBTC/USDC": [
            PriceQuote("aerodrome", "cbBTC/USDC", 68297.0, 0.0001, 1, time.time()),
            PriceQuote("uniswap",   "cbBTC/USDC", 68193.0, 0.0005, 1, time.time()),
        ]
    }
    profitable_opp = ArbOpportunity(
        pair="cbBTC/USDC", buy_venue="uniswap", sell_venue="aerodrome",
        buy_price=68193.0, sell_price=68297.0,
        gross_spread_pct=0.1516, total_fee_pct=0.06, net_spread_pct=0.0916,
        flash_loan_usdc=34000.0, estimated_profit_usdc=31.14,
        is_profitable=True, timestamp=time.time(),
    )
    mock_sim = SimResult(
        buy_dex="uniswap", sell_dex="aerodrome",
        token_amount=0.249, usdc_in=34000.0, usdc_out=34031.0,
        gross_profit_usd=31.0, gas_cost_usd=0.5, net_profit_usd=30.5,
        flash_provider="Morpho", is_executable=False,
        rejection_reason="EXECUTE_MODE=false",
    )
    w3_mock = MagicMock()

    logged_tags = []
    def mock_log(opp, tag, sim=None):
        logged_tags.append(tag)

    with patch("main.get_all_prices", return_value=mock_prices), \
         patch("main.detect_all_opportunities", return_value=[profitable_opp]), \
         patch("main.simulate_arb", return_value=mock_sim), \
         patch("main.log_opportunity", side_effect=mock_log), \
         patch("main.should_execute", return_value=(False, "EXECUTE_MODE=false")), \
         patch("main.config.MIN_NET_PROFIT_USD", 10.0), \
         patch("main.config.LOG_DIR", str(tmp_path)):
        main_module.run_cycle(w3_mock)

    assert "PROFITABLE" in logged_tags
