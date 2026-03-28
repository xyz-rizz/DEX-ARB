// SPDX-License-Identifier: MIT
pragma solidity 0.8.19;

// ─────────────────────────────────────────────────────────────────────────────
// Inline interfaces — no external imports.
// ─────────────────────────────────────────────────────────────────────────────

interface IERC20 {
    function approve(address spender, uint256 amount) external returns (bool);
    function transfer(address to, uint256 amount) external returns (bool);
    function balanceOf(address account) external view returns (uint256);
    function transferFrom(address from, address to, uint256 amount) external returns (bool);
}

interface IMorpho {
    function flashLoan(
        address token,
        uint256 assets,
        bytes calldata data
    ) external;
}

/// @dev Balancer V2 Vault flash loan interface
interface IBalancerVault {
    function flashLoan(
        address recipient,
        address[] memory tokens,
        uint256[] memory amounts,
        bytes memory userData
    ) external;
}

/// @dev Uniswap V3 SwapRouter02 — exactInputSingle
interface ISwapRouter {
    struct ExactInputSingleParams {
        address tokenIn;
        address tokenOut;
        uint24  fee;
        address recipient;
        uint256 amountIn;
        uint256 amountOutMinimum;
        uint160 sqrtPriceLimitX96;
    }
    function exactInputSingle(ExactInputSingleParams calldata params)
        external
        returns (uint256 amountOut);
}

/// @dev Aerodrome Slipstream router — identical struct to Uniswap V3 except
///      tickSpacing replaces fee.
interface IAeroRouter {
    struct ExactInputSingleParams {
        address tokenIn;
        address tokenOut;
        uint24  tickSpacing;
        address recipient;
        uint256 deadline;
        uint256 amountIn;
        uint256 amountOutMinimum;
        uint160 sqrtPriceLimitX96;
    }
    function exactInputSingle(ExactInputSingleParams calldata params)
        external
        returns (uint256 amountOut);
}

// ─────────────────────────────────────────────────────────────────────────────
// ArbExecutor — atomic flash-loan arbitrage between Aerodrome and Uniswap V3
// Supports both Morpho and Balancer V2 as flash loan providers.
// ─────────────────────────────────────────────────────────────────────────────

contract ArbExecutor {

    // ── Immutables ────────────────────────────────────────────────────────────
    address public immutable owner;

    // Base mainnet addresses — hardcoded for gas efficiency.
    address public constant MORPHO          = 0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb;
    address public constant BALANCER_VAULT  = 0xBA12222222228d8Ba445958a75a0704d566BF2C8;
    address public constant UNI_ROUTER      = 0x2626664c2603336E57B271c5C0b26F421741e481;
    address public constant AERO_ROUTER     = 0xBE6D8f0d05cC4be24d5167a3eF062215bE6D18a5;
    address public constant USDC            = 0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913;
    address public constant CBBTC           = 0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf;
    address public constant WETH            = 0x4200000000000000000000000000000000000006;
    address public constant WEETH           = 0x04C0599Ae5A44757c0af6F9eC3b93da8976c150A;

    // ── Provider enum ─────────────────────────────────────────────────────────
    uint8 public constant PROVIDER_MORPHO   = 0;
    uint8 public constant PROVIDER_BALANCER = 1;

    // ── Trade parameters struct ───────────────────────────────────────────────
    struct ArbParams {
        address tokenBorrow;       // token to flash-loan (typically USDC)
        address tokenIntermediate; // intermediate token (cbBTC, weETH, etc.)
        uint24  uniFee;            // Uniswap V3 fee tier (e.g. 500)
        uint24  aeroTickSpacing;   // Aerodrome tick spacing (e.g. 1)
        uint256 flashLoanAmount;   // raw units of tokenBorrow to borrow
        uint256 minIntermediate;   // min intermediate from first swap (slippage guard)
        uint256 minRepayToken;     // min borrow-token from second swap (slippage guard)
        uint256 minProfit;         // minimum profit in tokenBorrow — revert if below
        uint256 deadline;          // unix timestamp — revert if exceeded
        bool    buyOnUniswap;      // true → buy intermediate on Uni, sell on Aero
                                   // false → buy intermediate on Aero, sell on Uni
    }

    // Temporary storage for flash loan callback (re-entrancy guard doubles as storage)
    ArbParams private _pendingParams;
    bool private _inFlashLoan;

    // ── Events ─────────────────────────────────────────────────────────────────
    event ArbExecuted(
        address indexed tokenBorrow,
        uint256 flashLoanAmount,
        uint256 profit,
        uint8   provider
    );
    event EmergencyWithdraw(address token, uint256 amount, address to);

    // ── Constructor ────────────────────────────────────────────────────────────
    constructor() {
        owner = msg.sender;
    }

    // ── Access control ─────────────────────────────────────────────────────────
    modifier onlyOwner() {
        require(msg.sender == owner, "ArbExecutor: not owner");
        _;
    }

    // ── Main entry point ───────────────────────────────────────────────────────

    /**
     * @notice Execute an atomic arbitrage using the specified flash loan provider.
     * @param params    Trade parameters.
     * @param provider  0 = Morpho, 1 = Balancer.
     */
    function executeArb(ArbParams calldata params, uint8 provider) external onlyOwner {
        require(block.timestamp <= params.deadline, "ArbExecutor: deadline passed");
        require(params.flashLoanAmount > 0, "ArbExecutor: zero flash loan");
        require(!_inFlashLoan, "ArbExecutor: reentrant");

        _pendingParams = params;
        _inFlashLoan = true;

        if (provider == PROVIDER_MORPHO) {
            IMorpho(MORPHO).flashLoan(
                params.tokenBorrow,
                params.flashLoanAmount,
                abi.encode(provider)
            );
        } else if (provider == PROVIDER_BALANCER) {
            address[] memory tokens  = new address[](1);
            uint256[] memory amounts = new uint256[](1);
            tokens[0]  = params.tokenBorrow;
            amounts[0] = params.flashLoanAmount;
            IBalancerVault(BALANCER_VAULT).flashLoan(
                address(this),
                tokens,
                amounts,
                abi.encode(provider)
            );
        } else {
            revert("ArbExecutor: unknown provider");
        }

        _inFlashLoan = false;
    }

    // ── Morpho flash loan callback ─────────────────────────────────────────────

    /**
     * @notice Morpho flash loan callback.
     * @dev Called by Morpho immediately after sending tokenBorrow.
     *      Must approve Morpho to pull repayment before returning.
     */
    function onMorphoFlashLoan(uint256 assets, bytes calldata) external {
        require(msg.sender == MORPHO, "ArbExecutor: caller not Morpho");
        require(_inFlashLoan, "ArbExecutor: not in flash loan");

        _executeArbInternal(assets, PROVIDER_MORPHO);
    }

    // ── Balancer flash loan callback ───────────────────────────────────────────

    /**
     * @notice Balancer V2 flash loan callback.
     * @dev Called by Balancer Vault after sending tokens.
     *      Must repay (transfer back to Vault) before returning.
     *      feeAmounts is always zero on Base (Balancer charges 0% flash loan fee).
     */
    function receiveFlashLoan(
        address[] memory tokens,
        uint256[] memory amounts,
        uint256[] memory, /* feeAmounts — always 0 on Base */
        bytes memory
    ) external {
        require(msg.sender == BALANCER_VAULT, "ArbExecutor: caller not Balancer");
        require(_inFlashLoan, "ArbExecutor: not in flash loan");
        require(tokens.length == 1, "ArbExecutor: expected 1 token");

        uint256 assets = amounts[0];
        _executeArbInternal(assets, PROVIDER_BALANCER);

        // Balancer requires manual repayment via transfer back to Vault
        IERC20(tokens[0]).transfer(BALANCER_VAULT, assets);
    }

    // ── Internal arb execution (shared by both callbacks) ─────────────────────

    function _executeArbInternal(uint256 assets, uint8 provider) internal {
        ArbParams memory p = _pendingParams;
        require(block.timestamp <= p.deadline, "ArbExecutor: deadline in callback");

        uint256 intermediateReceived;

        if (p.buyOnUniswap) {
            // Step 1: tokenBorrow → tokenIntermediate on Uniswap V3
            IERC20(p.tokenBorrow).approve(UNI_ROUTER, assets);
            intermediateReceived = ISwapRouter(UNI_ROUTER).exactInputSingle(
                ISwapRouter.ExactInputSingleParams({
                    tokenIn:           p.tokenBorrow,
                    tokenOut:          p.tokenIntermediate,
                    fee:               p.uniFee,
                    recipient:         address(this),
                    amountIn:          assets,
                    amountOutMinimum:  p.minIntermediate,
                    sqrtPriceLimitX96: 0
                })
            );
            IERC20(p.tokenBorrow).approve(UNI_ROUTER, 0);

            // Step 2: tokenIntermediate → tokenBorrow on Aerodrome
            IERC20(p.tokenIntermediate).approve(AERO_ROUTER, intermediateReceived);
            IAeroRouter(AERO_ROUTER).exactInputSingle(
                IAeroRouter.ExactInputSingleParams({
                    tokenIn:           p.tokenIntermediate,
                    tokenOut:          p.tokenBorrow,
                    tickSpacing:       p.aeroTickSpacing,
                    recipient:         address(this),
                    deadline:          p.deadline,
                    amountIn:          intermediateReceived,
                    amountOutMinimum:  p.minRepayToken,
                    sqrtPriceLimitX96: 0
                })
            );
            IERC20(p.tokenIntermediate).approve(AERO_ROUTER, 0);

        } else {
            // Step 1: tokenBorrow → tokenIntermediate on Aerodrome
            IERC20(p.tokenBorrow).approve(AERO_ROUTER, assets);
            intermediateReceived = IAeroRouter(AERO_ROUTER).exactInputSingle(
                IAeroRouter.ExactInputSingleParams({
                    tokenIn:           p.tokenBorrow,
                    tokenOut:          p.tokenIntermediate,
                    tickSpacing:       p.aeroTickSpacing,
                    recipient:         address(this),
                    deadline:          p.deadline,
                    amountIn:          assets,
                    amountOutMinimum:  p.minIntermediate,
                    sqrtPriceLimitX96: 0
                })
            );
            IERC20(p.tokenBorrow).approve(AERO_ROUTER, 0);

            // Step 2: tokenIntermediate → tokenBorrow on Uniswap V3
            IERC20(p.tokenIntermediate).approve(UNI_ROUTER, intermediateReceived);
            ISwapRouter(UNI_ROUTER).exactInputSingle(
                ISwapRouter.ExactInputSingleParams({
                    tokenIn:           p.tokenIntermediate,
                    tokenOut:          p.tokenBorrow,
                    fee:               p.uniFee,
                    recipient:         address(this),
                    amountIn:          intermediateReceived,
                    amountOutMinimum:  p.minRepayToken,
                    sqrtPriceLimitX96: 0
                })
            );
            IERC20(p.tokenIntermediate).approve(UNI_ROUTER, 0);
        }

        // Step 3: Verify balance to cover repayment + profit
        uint256 balance = IERC20(p.tokenBorrow).balanceOf(address(this));

        if (provider == PROVIDER_MORPHO) {
            // Morpho pulls repayment — approve it
            require(balance >= assets, "ArbExecutor: insufficient to repay Morpho");
            IERC20(p.tokenBorrow).approve(MORPHO, assets);
            // balance check after approval:
            uint256 profit = balance - assets;
            require(profit >= p.minProfit, "ArbExecutor: profit below minimum");
            IERC20(p.tokenBorrow).transfer(owner, profit);
            emit ArbExecuted(p.tokenBorrow, assets, profit, provider);
        } else {
            // Balancer: receiveFlashLoan transfers repayment after this call returns
            // Here we just check we have enough and compute profit
            require(balance >= assets, "ArbExecutor: insufficient to repay Balancer");
            uint256 profit = balance - assets;
            require(profit >= p.minProfit, "ArbExecutor: profit below minimum");
            IERC20(p.tokenBorrow).transfer(owner, profit);
            emit ArbExecuted(p.tokenBorrow, assets, profit, provider);
            // NOTE: receiveFlashLoan() transfers 'assets' back to Vault after this returns
        }
    }

    // ── Emergency withdrawal ───────────────────────────────────────────────────

    function emergencyWithdraw(address token, uint256 amount) external onlyOwner {
        uint256 bal = IERC20(token).balanceOf(address(this));
        uint256 toSend = (amount == 0 || amount > bal) ? bal : amount;
        require(toSend > 0, "ArbExecutor: nothing to withdraw");
        IERC20(token).transfer(owner, toSend);
        emit EmergencyWithdraw(token, toSend, owner);
    }

    function emergencyWithdrawETH() external onlyOwner {
        uint256 bal = address(this).balance;
        require(bal > 0, "ArbExecutor: no ETH");
        (bool ok,) = owner.call{value: bal}("");
        require(ok, "ArbExecutor: ETH transfer failed");
    }

    receive() external payable {}
}
