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

/// @dev Aerodrome Slipstream router — identical struct to Uniswap V3.
/// Deployed at 0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43 on Base.
interface IAeroRouter {
    struct ExactInputSingleParams {
        address tokenIn;
        address tokenOut;
        uint24  tickSpacing;   // Aerodrome uses tickSpacing instead of fee
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
// ─────────────────────────────────────────────────────────────────────────────

contract ArbExecutor {

    // ── Immutables ────────────────────────────────────────────────────────────
    address public immutable owner;

    // Base mainnet addresses — hardcoded for gas efficiency.
    address public constant MORPHO      = 0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb;
    address public constant UNI_ROUTER  = 0x2626664c2603336E57B271c5C0b26F421741e481; // SwapRouter02
    address public constant AERO_ROUTER = 0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43;
    address public constant USDC        = 0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913;
    address public constant CBBTC       = 0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf;
    address public constant WETH        = 0x4200000000000000000000000000000000000006;
    address public constant WEETH       = 0x04C0599Ae5A44757c0af6F9eC3b93da8976c150A;

    // ── Trade parameters struct ───────────────────────────────────────────────
    struct ArbParams {
        address tokenBorrow;    // token to flash-loan (USDC or WETH)
        address tokenIntermediate; // token to receive from first swap (cbBTC or weETH)
        uint24  uniFee;         // Uniswap V3 fee tier (e.g. 500)
        int24   aeroTickSpacing;// Aerodrome tick spacing (e.g. 1)
        uint256 flashLoanAmount;// amount to borrow (in tokenBorrow raw units)
        uint256 minIntermediate;// min tokenIntermediate from first swap (slippage)
        uint256 minRepayToken;  // min tokenBorrow from second swap (slippage)
        uint256 minProfit;      // min profit in tokenBorrow — revert if below
        uint256 deadline;       // unix timestamp — revert if exceeded
        bool    buyOnUniswap;   // true = buy intermediate on Uni, sell on Aero
                                // false = buy intermediate on Aero, sell on Uni
    }

    // Temporary storage for flash loan callback
    ArbParams private _pendingParams;
    bool private _inFlashLoan;

    // ── Events ─────────────────────────────────────────────────────────────────
    event ArbExecuted(
        address indexed tokenBorrow,
        uint256 flashLoanAmount,
        uint256 profit
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
     * @notice Execute an atomic arbitrage via Morpho flash loan.
     * @dev Caller must be owner. Morpho calls back onMorphoFlashLoan().
     *      The full trade is atomic — if profit < minProfit, entire tx reverts.
     */
    function executeArb(ArbParams calldata params) external onlyOwner {
        require(block.timestamp <= params.deadline, "ArbExecutor: deadline passed");
        require(params.flashLoanAmount > 0, "ArbExecutor: zero flash loan");
        require(!_inFlashLoan, "ArbExecutor: reentrant");

        _pendingParams = params;
        _inFlashLoan = true;

        // Morpho will call onMorphoFlashLoan() synchronously
        IMorpho(MORPHO).flashLoan(
            params.tokenBorrow,
            params.flashLoanAmount,
            ""  // data unused — params passed via storage
        );

        _inFlashLoan = false;
    }

    /**
     * @notice Morpho flash loan callback.
     * @dev Called by Morpho immediately after sending tokens.
     *      Must repay flashLoanAmount before returning.
     *      Flow (buyOnUniswap=true):
     *        USDC → [Uniswap] → cbBTC → [Aerodrome] → USDC → repay Morpho
     *      Flow (buyOnUniswap=false):
     *        USDC → [Aerodrome] → cbBTC → [Uniswap] → USDC → repay Morpho
     */
    function onMorphoFlashLoan(uint256 assets, bytes calldata) external {
        require(msg.sender == MORPHO, "ArbExecutor: caller not Morpho");
        require(_inFlashLoan, "ArbExecutor: not in flash loan");

        ArbParams memory p = _pendingParams;
        require(block.timestamp <= p.deadline, "ArbExecutor: deadline in callback");

        uint256 intermediateReceived;

        if (p.buyOnUniswap) {
            // Step 1: USDC → cbBTC on Uniswap V3
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
            // Clear approval
            IERC20(p.tokenBorrow).approve(UNI_ROUTER, 0);

            // Step 2: cbBTC → USDC on Aerodrome
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
            // Step 1: USDC → cbBTC on Aerodrome
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

            // Step 2: cbBTC → USDC on Uniswap V3
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

        // Step 3: Check we have enough to repay
        uint256 balance = IERC20(p.tokenBorrow).balanceOf(address(this));
        require(balance >= assets, "ArbExecutor: insufficient to repay");

        // Step 4: Repay Morpho flash loan
        IERC20(p.tokenBorrow).approve(MORPHO, assets);
        // Morpho pulls the repayment — nothing more to do here.

        // Step 5: Check minimum profit
        uint256 profit = balance - assets;
        require(profit >= p.minProfit, "ArbExecutor: profit below minimum");

        // Step 6: Transfer profit to owner
        IERC20(p.tokenBorrow).transfer(owner, profit);

        emit ArbExecuted(p.tokenBorrow, assets, profit);
    }

    // ── Emergency withdrawal ───────────────────────────────────────────────────

    /**
     * @notice Emergency: withdraw any ERC-20 token stuck in the contract.
     * @dev Owner only. Should not be needed in normal operation.
     */
    function emergencyWithdraw(address token, uint256 amount) external onlyOwner {
        uint256 bal = IERC20(token).balanceOf(address(this));
        uint256 toSend = (amount == 0 || amount > bal) ? bal : amount;
        require(toSend > 0, "ArbExecutor: nothing to withdraw");
        IERC20(token).transfer(owner, toSend);
        emit EmergencyWithdraw(token, toSend, owner);
    }

    /**
     * @notice Emergency: withdraw native ETH.
     */
    function emergencyWithdrawETH() external onlyOwner {
        uint256 bal = address(this).balance;
        require(bal > 0, "ArbExecutor: no ETH");
        (bool ok, ) = owner.call{value: bal}("");
        require(ok, "ArbExecutor: ETH transfer failed");
    }

    receive() external payable {}
}
