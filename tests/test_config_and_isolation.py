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
        main_module.run_cycle(w3_mock, None, stats)

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
        flash_provider="Morpho", is_executable=True,
        rejection_reason="",
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

    assert "SKIP" in logged_tags


# ── New: lifecycle ordering + JSONL gate tests ────────────────────────────────

def test_arb_scan_logged_after_simulation():
    """simulate_arb must be called BEFORE _log_scan_line in run_cycle."""
    import main as main_module
    from price_scanner import PriceQuote
    from arb_detector import ArbOpportunity, SimResult

    profitable_opp = ArbOpportunity(
        pair="cbBTC/USDC", buy_venue="uniswap", sell_venue="aerodrome",
        buy_price=68193.0, sell_price=68297.0,
        gross_spread_pct=0.15, total_fee_pct=0.06, net_spread_pct=0.09,
        flash_loan_usdc=34000.0, estimated_profit_usdc=30.0,
        is_profitable=True, timestamp=time.time(),
    )
    mock_sim = SimResult(
        buy_dex="uniswap", sell_dex="aerodrome",
        token_amount=0.1, usdc_in=34000.0, usdc_out=34030.0,
        gross_profit_usd=30.0, gas_cost_usd=0.5, net_profit_usd=29.5,
        flash_provider="Morpho", is_executable=False, rejection_reason="no_contract",
    )
    mock_prices = {"cbBTC/USDC": [
        PriceQuote("aerodrome", "cbBTC/USDC", 68297.0, 0.0001, 1, time.time()),
        PriceQuote("uniswap",   "cbBTC/USDC", 68193.0, 0.0005, 1, time.time()),
    ]}

    call_order = []

    def record_sim(w3, opp):
        call_order.append("simulate")
        return mock_sim

    def record_scan(opp, sim=None):
        call_order.append("scan")

    w3_mock = MagicMock()
    with patch("main.get_all_prices", return_value=mock_prices), \
         patch("main.detect_all_opportunities", return_value=[profitable_opp]), \
         patch("main.simulate_arb", side_effect=record_sim), \
         patch("main._log_scan_line", side_effect=record_scan), \
         patch("main.should_execute", return_value=(False, "no_contract")):
        main_module.run_cycle(w3_mock)

    assert "simulate" in call_order, "simulate_arb must be called"
    assert "scan" in call_order, "_log_scan_line must be called"
    sim_idx  = call_order.index("simulate")
    scan_idx = call_order.index("scan")
    assert sim_idx < scan_idx, f"simulate must precede scan; order={call_order}"


def test_executions_jsonl_only_after_sim_pass(tmp_path):
    """log_opportunity must NOT be called when sim.is_executable=False."""
    import main as main_module
    from price_scanner import PriceQuote
    from arb_detector import ArbOpportunity, SimResult

    profitable_opp = ArbOpportunity(
        pair="cbBTC/USDC", buy_venue="uniswap", sell_venue="aerodrome",
        buy_price=68193.0, sell_price=68297.0,
        gross_spread_pct=0.15, total_fee_pct=0.06, net_spread_pct=0.09,
        flash_loan_usdc=34000.0, estimated_profit_usdc=30.0,
        is_profitable=True, timestamp=time.time(),
    )
    failing_sim = SimResult(
        buy_dex="uniswap", sell_dex="aerodrome",
        token_amount=0.1, usdc_in=34000.0, usdc_out=34005.0,
        gross_profit_usd=5.0, gas_cost_usd=2.0, net_profit_usd=3.0,
        flash_provider="Morpho", is_executable=False, rejection_reason="below_min_profit",
    )
    mock_prices = {"cbBTC/USDC": [
        PriceQuote("aerodrome", "cbBTC/USDC", 68297.0, 0.0001, 1, time.time()),
        PriceQuote("uniswap",   "cbBTC/USDC", 68193.0, 0.0005, 1, time.time()),
    ]}

    log_calls = []
    w3_mock = MagicMock()
    with patch("main.get_all_prices", return_value=mock_prices), \
         patch("main.detect_all_opportunities", return_value=[profitable_opp]), \
         patch("main.simulate_arb", return_value=failing_sim), \
         patch("main.log_opportunity",
               side_effect=lambda opp, tag, sim=None: log_calls.append(tag)), \
         patch("main.config.LOG_DIR", str(tmp_path)):
        main_module.run_cycle(w3_mock)

    assert len(log_calls) == 0, \
        f"log_opportunity must not be called when sim fails; got: {log_calls}"


def test_prime_requires_sim_pass(tmp_path):
    """A PRIME opp is logged as 'SKIP' when sim passes but should_execute returns False."""
    import main as main_module
    from price_scanner import PriceQuote
    from arb_detector import ArbOpportunity, SimResult

    prime_opp = ArbOpportunity(
        pair="cbBTC/USDC", buy_venue="uniswap", sell_venue="aerodrome",
        buy_price=68000.0, sell_price=69000.0,
        gross_spread_pct=1.47, total_fee_pct=0.06, net_spread_pct=1.41,
        flash_loan_usdc=50000.0, estimated_profit_usdc=705.0,
        is_profitable=True, timestamp=time.time(), tier="PRIME",
    )
    passing_sim = SimResult(
        buy_dex="uniswap", sell_dex="aerodrome",
        token_amount=0.5, usdc_in=50000.0, usdc_out=50700.0,
        gross_profit_usd=700.0, gas_cost_usd=1.0, net_profit_usd=699.0,
        flash_provider="Morpho", is_executable=True, rejection_reason="",
    )
    mock_prices = {"cbBTC/USDC": [
        PriceQuote("aerodrome", "cbBTC/USDC", 69000.0, 0.0001, 1, time.time()),
        PriceQuote("uniswap",   "cbBTC/USDC", 68000.0, 0.0005, 1, time.time()),
    ]}

    logged_tags = []
    w3_mock = MagicMock()
    with patch("main.get_all_prices", return_value=mock_prices), \
         patch("main.detect_all_opportunities", return_value=[prime_opp]), \
         patch("main.simulate_arb", return_value=passing_sim), \
         patch("main.log_opportunity",
               side_effect=lambda opp, tag, sim=None: logged_tags.append(tag)), \
         patch("main.should_execute", return_value=(False, "EXECUTE_MODE=false")), \
         patch("main.config.LOG_DIR", str(tmp_path)):
        main_module.run_cycle(w3_mock)

    assert "SKIP" in logged_tags, "sim-passed PRIME opp must be logged as SKIP"
    assert "PROFITABLE" not in logged_tags, "PROFITABLE tag must not be used"


def test_alchemy_key_is_new_key():
    """ALCHEMY_EXEC_URL must not contain the old rotated Alchemy key."""
    old_key = "pDNSLfjTbJYOD9RdmWGGY"
    assert old_key not in (config.ALCHEMY_EXEC_URL or ""), \
        f"Old Alchemy key still present in ALCHEMY_EXEC_URL: {config.ALCHEMY_EXEC_URL}"
