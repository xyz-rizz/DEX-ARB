"""
Tests for executor.py — all offline.
"""

import json
import os
import time
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from arb_detector import ArbOpportunity, SimResult
from executor import should_execute, log_opportunity


def _make_opp(profitable=True, profit=31.14, pair="cbBTC/USDC",
              tier="MARGINAL"):
    return ArbOpportunity(
        pair=pair,
        buy_venue="Uniswap V3",
        sell_venue="Aerodrome Slipstream",
        buy_price=68193.69,
        sell_price=68297.04,
        gross_spread_pct=0.1516,
        total_fee_pct=0.06,
        net_spread_pct=0.0916,
        flash_loan_usdc=17000.0,
        estimated_profit_usdc=profit,
        is_profitable=profitable,
        timestamp=time.time(),
        tier=tier,
    )


def _make_sim(executable=True, reason="", net_profit=15.0):
    return SimResult(
        buy_dex="Uniswap V3",
        sell_dex="Aerodrome Slipstream",
        token_amount=0.249,
        usdc_in=17000.0,
        usdc_out=17015.5,
        gross_profit_usd=15.5,
        gas_cost_usd=0.5,
        net_profit_usd=net_profit,
        flash_provider="Morpho",
        is_executable=executable,
        rejection_reason=reason,
    )


# ── should_execute ─────────────────────────────────────────────────────────────

def test_should_execute_false_when_execute_mode_off():
    """EXECUTE_MODE=false → should_execute returns False."""
    opp = _make_opp(profitable=True, profit=50.0)
    with patch("executor.config.EXECUTE_MODE", False):
        result, reason = should_execute(opp)
    assert result is False
    assert "blocked_by_execute_mode" in reason


def test_should_execute_false_when_no_contract_address():
    """No ARB_EXECUTOR_ADDRESS → should_execute returns False."""
    opp = _make_opp(profitable=True, profit=50.0)
    with patch("executor.config.EXECUTE_MODE", True), \
         patch("executor.config.ARB_EXECUTOR_ADDRESS", ""):
        result, reason = should_execute(opp)
    assert result is False
    assert "blocked_by_no_contract" in reason


def test_should_execute_false_when_below_min_profit():
    """Profit below MIN_NET_PROFIT_USD → should_execute returns False."""
    opp = _make_opp(profitable=True, profit=2.0)
    with patch("executor.config.EXECUTE_MODE", True), \
         patch("executor.config.ARB_EXECUTOR_ADDRESS", "0xDEAD"), \
         patch("executor.config.MIN_NET_PROFIT_USD", 10.0):
        result, reason = should_execute(opp)
    assert result is False
    assert "profit" in reason.lower() or "$2.00" in reason


def test_should_execute_false_when_not_profitable():
    """is_profitable=False → should_execute returns False."""
    opp = _make_opp(profitable=False, profit=50.0)
    with patch("executor.config.EXECUTE_MODE", True), \
         patch("executor.config.ARB_EXECUTOR_ADDRESS", "0xDEAD"):
        result, reason = should_execute(opp)
    assert result is False
    assert "profitable" in reason.lower()


def test_should_execute_true_when_all_gates_pass():
    """All gates pass → should_execute returns (True, '')."""
    opp = _make_opp(profitable=True, profit=50.0)
    sim = _make_sim(executable=True)
    with patch("executor.config.EXECUTE_MODE", True), \
         patch("executor.config.ARB_EXECUTOR_ADDRESS", "0xDEAD1234"), \
         patch("executor.config.MIN_NET_PROFIT_USD", 10.0):
        result, reason = should_execute(opp, sim)
    assert result is True
    assert reason == ""


# ── log_opportunity ────────────────────────────────────────────────────────────

def test_log_opportunity_writes_jsonl(tmp_path):
    """log_opportunity must write to executions.jsonl."""
    opp = _make_opp()
    with patch("executor.config.LOG_DIR", str(tmp_path)):
        log_opportunity(opp, "PROFITABLE")

    jsonl_path = tmp_path / "executions.jsonl"
    assert jsonl_path.exists(), "executions.jsonl not created"
    lines = jsonl_path.read_text().strip().split("\n")
    assert len(lines) == 1


def test_log_opportunity_all_fields_present(tmp_path):
    """Every required field must be present in the logged JSONL record."""
    opp = _make_opp()
    with patch("executor.config.LOG_DIR", str(tmp_path)):
        log_opportunity(opp, "PROFITABLE")

    record = json.loads((tmp_path / "executions.jsonl").read_text().strip())

    required = [
        "timestamp", "tag", "pair", "buy_venue", "sell_venue",
        "buy_price", "sell_price", "gross_spread_pct", "net_spread_pct",
        "flash_loan_usdc", "estimated_profit_usdc", "tx_hash",
        "actual_profit_usdc", "error",
    ]
    for f in required:
        assert f in record, f"Missing field: {f}"


def test_jsonl_parseable(tmp_path):
    """Multiple logged records must all be valid JSON lines."""
    opp = _make_opp()
    tags = ["PROFITABLE", "DRY", "SKIP", "BELOW_THRESHOLD"]
    with patch("executor.config.LOG_DIR", str(tmp_path)):
        for tag in tags:
            log_opportunity(opp, tag)

    lines = (tmp_path / "executions.jsonl").read_text().strip().split("\n")
    assert len(lines) == len(tags)
    for i, line in enumerate(lines):
        record = json.loads(line)
        assert record["tag"] == tags[i]


def test_log_opportunity_tag_stored(tmp_path):
    """The tag in the JSONL record must match what was passed."""
    opp = _make_opp()
    for tag in ("PROFITABLE", "DRY", "SKIP", "ERROR", "BELOW_THRESHOLD"):
        with patch("executor.config.LOG_DIR", str(tmp_path)):
            log_opportunity(opp, tag)

    records = [json.loads(l) for l in (tmp_path / "executions.jsonl").read_text().strip().split("\n")]
    stored_tags = [r["tag"] for r in records]
    assert "PROFITABLE" in stored_tags
    assert "DRY" in stored_tags


# ── New simulation gate tests ──────────────────────────────────────────────────

def test_simulation_gate_blocks_execution():
    """should_execute returns False when sim.is_executable=False."""
    opp = _make_opp(profitable=True, profit=50.0)
    sim = _make_sim(executable=False, reason="below_min_profit:2.00<10.0")
    with patch("executor.config.EXECUTE_MODE", True), \
         patch("executor.config.ARB_EXECUTOR_ADDRESS", "0xDEAD"), \
         patch("executor.config.MIN_NET_PROFIT_USD", 10.0):
        result, reason = should_execute(opp, sim)
    assert result is False
    assert "blocked_by_sim_rejection" in reason


def test_execution_uses_sim_result_amounts(tmp_path):
    """log_opportunity records sim_net_profit_usd from SimResult."""
    opp = _make_opp()
    sim = _make_sim(net_profit=42.0)
    with patch("executor.config.LOG_DIR", str(tmp_path)):
        log_opportunity(opp, "PROFITABLE", sim)

    record = json.loads((tmp_path / "executions.jsonl").read_text().strip())
    assert "sim_net_profit_usd" in record
    assert abs(record["sim_net_profit_usd"] - 42.0) < 0.01


# ── Config and isolation tests ─────────────────────────────────────────────────

def test_config_validates_on_missing_key():
    """validate() raises ValueError when BASE_RPC_URL is empty."""
    import config as cfg
    with patch.object(cfg, "BASE_RPC_URL", ""), \
         patch.object(cfg, "EXECUTE_MODE", False):
        with pytest.raises(ValueError, match="BASE_RPC_URL"):
            cfg.validate()


def test_config_validates_execute_mode_needs_key():
    """validate() raises ValueError when EXECUTE_MODE=true but PRIVATE_KEY empty."""
    import config as cfg
    with patch.object(cfg, "BASE_RPC_URL", "http://rpc"), \
         patch.object(cfg, "EXECUTE_MODE", True), \
         patch.object(cfg, "PRIVATE_KEY", ""), \
         patch.object(cfg, "ARB_EXECUTOR_ADDRESS", "0xDEAD"):
        with pytest.raises(ValueError, match="PRIVATE_KEY"):
            cfg.validate()


def test_config_all_addresses_checksummed():
    """All address constants in config must be EIP-55 checksummed."""
    from web3 import Web3
    import config as cfg

    addr_attrs = [
        "USDC_ADDRESS", "CBBTC_ADDRESS", "WETH_ADDRESS", "WEETH_ADDRESS",
        "UNISWAP_QUOTER_V2", "UNISWAP_FACTORY", "AERODROME_ROUTER",
        "AERODROME_FACTORY", "MORPHO_ADDRESS", "BALANCER_VAULT",
    ]
    for attr in addr_attrs:
        addr = getattr(cfg, attr)
        assert addr == Web3.to_checksum_address(addr), \
            f"{attr}={addr} is not checksummed"


def test_no_import_from_existing_bot():
    """New project must not import from morpho_scanner (checks import statements, not comments)."""
    import ast, os
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    violations = []
    for root, _, files in os.walk(project_dir):
        if "venv" in root:
            continue
        for fname in files:
            if not fname.endswith(".py"):
                continue
            fpath = os.path.join(root, fname)
            try:
                tree = ast.parse(open(fpath, encoding="utf-8", errors="ignore").read())
            except SyntaxError:
                continue
            for node in ast.walk(tree):
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    module = ""
                    if isinstance(node, ast.Import):
                        module = ",".join(a.name for a in node.names)
                    elif isinstance(node, ast.ImportFrom) and node.module:
                        module = node.module
                    if "morpho_scanner" in module:
                        violations.append(f"{fpath}:{node.lineno}")
    assert violations == [], f"morpho_scanner import found: {violations}"


def test_main_loop_does_not_crash_on_price_error():
    """run_cycle must not raise even if get_all_prices throws."""
    from main import run_cycle, CycleStats
    w3 = MagicMock()
    stats = CycleStats()

    with patch("main.get_all_prices", side_effect=Exception("RPC error")):
        # Must not raise
        run_cycle(w3, None, stats)

    assert stats.cycles == 0  # Nothing recorded since prices failed


# ── New: EXECUTION_READY + ExecutionResult tests ──────────────────────────────

def test_execution_ready_false_when_no_address():
    """EXECUTION_READY matches whether ARB_EXECUTOR_ADDRESS is set."""
    import config as cfg
    expected = bool(cfg.ARB_EXECUTOR_ADDRESS.strip())
    assert cfg.EXECUTION_READY == expected


def test_execute_arb_stub_when_no_contract():
    """execute_arb returns ExecutionResult(tag='STUB') when EXECUTION_READY=False."""
    from executor import execute_arb, ExecutionResult
    opp = _make_opp(profitable=True, profit=50.0)
    sim = _make_sim(executable=True)
    w3_mock = MagicMock()

    with patch("executor.config.EXECUTION_READY", False):
        result = execute_arb(w3_mock, opp, sim)

    assert isinstance(result, ExecutionResult)
    assert result.tag == "STUB"
    assert result.reason == "no_contract_address"
    assert result.estimated_profit_usd == sim.net_profit_usd


# ── Aerodrome router / tickSpacing / deadline fix tests ────────────────────────

def test_slipstream_uses_correct_router():
    """config.py must reference the Slipstream SwapRouter (0xBE6D...), not the vAMM router (0xcf77...)."""
    import config as cfg
    slipstream_router = cfg.AERODROME_SLIPSTREAM_ROUTER.lower()
    assert "be6d8f0d" in slipstream_router, \
        f"Slipstream SwapRouter not in config; got {cfg.AERODROME_SLIPSTREAM_ROUTER}"


def test_slipstream_dex_config_uses_correct_router():
    """DEX_CONFIG 'Aerodrome Slipstream' entry must use SwapRouter (0xBE6D...), not vAMM router."""
    import config as cfg
    slipstream_entry = next(
        (d for d in cfg._BASE_DEX_CONFIG if d["name"] == "Aerodrome Slipstream"), None
    )
    assert slipstream_entry is not None, "Aerodrome Slipstream not in _BASE_DEX_CONFIG"
    router = slipstream_entry["router"].lower()
    assert "be6d8f0d" in router, \
        f"Aerodrome Slipstream DEX entry has wrong router: {slipstream_entry['router']}"
    assert "cf77a3ba" not in router, \
        f"Aerodrome Slipstream DEX entry still uses vAMM router"


def test_wrong_router_not_used_for_exactinputsingle():
    """executor._PAIR_EXEC_PARAMS must not silently use default tick_spacing=50 for active pairs."""
    from executor import _PAIR_EXEC_PARAMS, _DEFAULT_EXEC_PARAMS
    import config as cfg
    active_pairs = [p["name"] for p in cfg._BASE_PAIR_CONFIG]
    for pair in active_pairs:
        ep = _PAIR_EXEC_PARAMS.get(pair, _DEFAULT_EXEC_PARAMS)
        tick = ep["aero_tick"]
        assert tick not in (500, 3000, 10000), \
            f"{pair} has fee tier {tick} as aero_tick — must be actual tickSpacing"


def test_tick_spacing_weth_usdc():
    """WETH/USDC Slipstream tick spacing must be 100, not 200."""
    from executor import _PAIR_EXEC_PARAMS
    assert "WETH/USDC" in _PAIR_EXEC_PARAMS, "WETH/USDC missing from _PAIR_EXEC_PARAMS"
    tick = _PAIR_EXEC_PARAMS["WETH/USDC"]["aero_tick"]
    assert tick == 100, f"Expected aero_tick=100 for WETH/USDC, got {tick}"


def test_tick_spacing_cbbtc_usdc():
    """cbBTC/USDC Slipstream tick spacing must be 1."""
    from executor import _PAIR_EXEC_PARAMS
    tick = _PAIR_EXEC_PARAMS["cbBTC/USDC"]["aero_tick"]
    assert tick == 1, f"Expected aero_tick=1 for cbBTC/USDC, got {tick}"


def test_tick_spacing_cbbtc_weth():
    """cbBTC/WETH Slipstream tick spacing must be 100."""
    from executor import _PAIR_EXEC_PARAMS
    assert "cbBTC/WETH" in _PAIR_EXEC_PARAMS, "cbBTC/WETH missing from _PAIR_EXEC_PARAMS"
    tick = _PAIR_EXEC_PARAMS["cbBTC/WETH"]["aero_tick"]
    assert tick == 100, f"Expected aero_tick=100 for cbBTC/WETH, got {tick}"


def test_deadline_is_dynamic():
    """_build_arb_params must produce a deadline > now+60 and < now+600."""
    import time
    from executor import _build_arb_params, _PAIR_EXEC_PARAMS
    import config as cfg
    from arb_detector import ArbOpportunity, SimResult

    opp = ArbOpportunity(
        pair="cbBTC/USDC",
        buy_venue="Uniswap V3",
        sell_venue="Aerodrome Slipstream",
        buy_price=65000.0, sell_price=65100.0,
        gross_spread_pct=0.15, total_fee_pct=0.06, net_spread_pct=0.09,
        flash_loan_usdc=17000.0, estimated_profit_usdc=15.0,
        is_profitable=True, timestamp=time.time(), tier="MARGINAL",
    )
    sim = SimResult(
        buy_dex="Uniswap V3", sell_dex="Aerodrome Slipstream",
        token_amount=0.249, usdc_in=17000.0, usdc_out=17015.0,
        gross_profit_usd=15.0, gas_cost_usd=0.5, net_profit_usd=14.5,
        flash_provider="Morpho", is_executable=True, rejection_reason="",
    )

    before = int(time.time())
    params = _build_arb_params(opp, sim, eth_price=2000.0)
    deadline = params[11]  # index 11 = deadline (new 12-field struct)

    assert deadline > before + 60, f"Deadline not far enough in future: {deadline} vs now={before}"
    assert deadline < before + 600, f"Deadline suspiciously far in future: {deadline}"


# ── Venue ID / PancakeSwap routing tests ──────────────────────────────────────

def test_venue_id_uniswap():
    """Uniswap V3 buy/sell venue → VENUE_UNI (0)."""
    from executor import _venue_id, _VENUE_UNI
    assert _venue_id("Uniswap V3") == _VENUE_UNI
    assert _venue_id("Uniswap") == _VENUE_UNI


def test_venue_id_pancakeswap():
    """PancakeSwap V3 buy/sell venue → VENUE_CAKE (1)."""
    from executor import _venue_id, _VENUE_CAKE
    assert _venue_id("PancakeSwap V3") == _VENUE_CAKE
    assert _venue_id("PancakeSwap") == _VENUE_CAKE


def test_venue_id_aerodrome():
    """Aerodrome Slipstream venue → VENUE_AERO (2)."""
    from executor import _venue_id, _VENUE_AERO
    assert _venue_id("Aerodrome Slipstream") == _VENUE_AERO
    assert _venue_id("Aerodrome") == _VENUE_AERO


def test_venue_id_unknown_defaults_to_uni():
    """Unknown venue name defaults to VENUE_UNI to avoid silent mismatch."""
    from executor import _venue_id, _VENUE_UNI
    assert _venue_id("BaseSwap") == _VENUE_UNI
    assert _venue_id("SomeNewDex") == _VENUE_UNI


def test_build_arb_params_pancake_buy_sets_venue_cake():
    """PancakeSwap buy venue → buyVenueId=VENUE_CAKE at params[5]."""
    import time
    from executor import _build_arb_params, _VENUE_CAKE, _VENUE_AERO
    from arb_detector import ArbOpportunity, SimResult

    opp = ArbOpportunity(
        pair="WETH/USDC",
        buy_venue="PancakeSwap V3",
        sell_venue="Aerodrome Slipstream",
        buy_price=2000.0, sell_price=2010.0,
        gross_spread_pct=0.5, total_fee_pct=0.06, net_spread_pct=0.44,
        flash_loan_usdc=5000.0, estimated_profit_usdc=22.0,
        is_profitable=True, timestamp=time.time(), tier="GOOD",
    )
    sim = SimResult(
        buy_dex="PancakeSwap V3", sell_dex="Aerodrome Slipstream",
        token_amount=2.49, usdc_in=5000.0, usdc_out=5022.0,
        gross_profit_usd=22.0, gas_cost_usd=0.5, net_profit_usd=21.5,
        flash_provider="Morpho", is_executable=True, rejection_reason="",
    )
    params = _build_arb_params(opp, sim, eth_price=2000.0)

    assert params[5] == _VENUE_CAKE, f"buyVenueId expected VENUE_CAKE={_VENUE_CAKE}, got {params[5]}"
    assert params[6] == _VENUE_AERO, f"sellVenueId expected VENUE_AERO={_VENUE_AERO}, got {params[6]}"


def test_build_arb_params_uni_buy_aero_sell():
    """Uniswap buy + Aerodrome sell → correct venue IDs at params[5] and params[6]."""
    import time
    from executor import _build_arb_params, _VENUE_UNI, _VENUE_AERO
    from arb_detector import ArbOpportunity, SimResult

    opp = ArbOpportunity(
        pair="cbBTC/USDC",
        buy_venue="Uniswap V3",
        sell_venue="Aerodrome Slipstream",
        buy_price=65000.0, sell_price=65200.0,
        gross_spread_pct=0.31, total_fee_pct=0.06, net_spread_pct=0.25,
        flash_loan_usdc=17000.0, estimated_profit_usdc=42.0,
        is_profitable=True, timestamp=time.time(), tier="GOOD",
    )
    sim = SimResult(
        buy_dex="Uniswap V3", sell_dex="Aerodrome Slipstream",
        token_amount=0.249, usdc_in=17000.0, usdc_out=17042.0,
        gross_profit_usd=42.0, gas_cost_usd=0.5, net_profit_usd=41.5,
        flash_provider="Morpho", is_executable=True, rejection_reason="",
    )
    params = _build_arb_params(opp, sim, eth_price=2000.0)

    assert params[5] == _VENUE_UNI,  f"buyVenueId={params[5]}, want {_VENUE_UNI}"
    assert params[6] == _VENUE_AERO, f"sellVenueId={params[6]}, want {_VENUE_AERO}"


def test_build_arb_params_has_12_fields():
    """_build_arb_params must return exactly 12 elements (new struct)."""
    import time
    from executor import _build_arb_params
    from arb_detector import ArbOpportunity, SimResult

    opp = ArbOpportunity(
        pair="WETH/USDC",
        buy_venue="Uniswap V3",
        sell_venue="Aerodrome Slipstream",
        buy_price=2000.0, sell_price=2010.0,
        gross_spread_pct=0.5, total_fee_pct=0.06, net_spread_pct=0.44,
        flash_loan_usdc=5000.0, estimated_profit_usdc=22.0,
        is_profitable=True, timestamp=time.time(), tier="GOOD",
    )
    sim = SimResult(
        buy_dex="Uniswap V3", sell_dex="Aerodrome Slipstream",
        token_amount=2.49, usdc_in=5000.0, usdc_out=5022.0,
        gross_profit_usd=22.0, gas_cost_usd=0.5, net_profit_usd=21.5,
        flash_provider="Morpho", is_executable=True, rejection_reason="",
    )
    params = _build_arb_params(opp, sim, eth_price=2000.0)
    assert len(params) == 12, f"Expected 12-field tuple, got {len(params)}: {params}"


def test_pair_exec_params_has_cake_fee():
    """Every active pair in PAIR_EXEC_PARAMS must have a cake_fee field."""
    import config as cfg
    for pair_name, ep in cfg._BASE_PAIR_EXEC_PARAMS.items():
        assert "cake_fee" in ep, f"{pair_name} missing cake_fee in PAIR_EXEC_PARAMS"
        assert ep["cake_fee"] > 0, f"{pair_name}.cake_fee must be > 0"


def test_pair_exec_params_no_fee_tier_as_aero_tick():
    """aero_tick must be a Slipstream tick spacing (1/50/100/200), not a Uni fee tier."""
    import config as cfg
    valid_ticks = {1, 50, 100, 200}
    for pair_name, ep in cfg._BASE_PAIR_EXEC_PARAMS.items():
        tick = ep["aero_tick"]
        assert tick in valid_ticks, (
            f"{pair_name}.aero_tick={tick} is not a valid Slipstream tick spacing "
            f"(must be one of {sorted(valid_ticks)})"
        )


def test_pancakeswap_v3_router_in_config():
    """config.PANCAKESWAP_V3_ROUTER must be set and checksummed."""
    from web3 import Web3
    import config as cfg
    addr = cfg.PANCAKESWAP_V3_ROUTER
    assert addr, "PANCAKESWAP_V3_ROUTER is empty"
    assert addr == Web3.to_checksum_address(addr), f"Not checksummed: {addr}"
    assert "1b81" in addr.lower(), f"Unexpected PANCAKESWAP_V3_ROUTER address: {addr}"


def test_uni_fee_tiers_deep_pool_first():
    """Uniswap V3 fee_tiers must start with 500 (deepest WETH/USDC pool), not 100."""
    import config as cfg
    uni = next((d for d in cfg._BASE_DEX_CONFIG if d["name"] == "Uniswap V3"), None)
    assert uni is not None, "Uniswap V3 not in _BASE_DEX_CONFIG"
    assert uni["fee_tiers"][0] == 500, (
        f"Expected fee_tiers[0]=500 (deepest pool first), got {uni['fee_tiers']}"
    )


def test_pancake_fee_tiers_deep_pool_first():
    """PancakeSwap V3 fee_tiers must start with 500 (deepest pool), not 100."""
    import config as cfg
    cake = next((d for d in cfg._BASE_DEX_CONFIG if d["name"] == "PancakeSwap V3"), None)
    assert cake is not None, "PancakeSwap V3 not in _BASE_DEX_CONFIG"
    assert cake["fee_tiers"][0] == 500, (
        f"Expected fee_tiers[0]=500 (deepest pool first), got {cake['fee_tiers']}"
    )
