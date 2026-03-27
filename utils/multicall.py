"""
Multicall3 helper: collapse N eth_call reads into one RPC round-trip.

Canonical address on every EVM chain (including Base mainnet):
    0xcA11bde05977b3631167028862bE2a173976CA11

Uses aggregate3 with allowFailure=True on every subcall so one failed
subcall never aborts the batch.  Failed subcalls return None in the list.

IMPORTANT: always pass the dRPC (read) w3 instance here.
           Never pass the Alchemy (execution) w3.
"""

from web3 import Web3

MULTICALL3_ADDRESS = "0xcA11bde05977b3631167028862bE2a173976CA11"

MULTICALL3_ABI = [
    {
        "name": "aggregate3",
        "type": "function",
        "inputs": [
            {
                "name": "calls",
                "type": "tuple[]",
                "components": [
                    {"name": "target",       "type": "address"},
                    {"name": "allowFailure", "type": "bool"},
                    {"name": "callData",     "type": "bytes"},
                ],
            }
        ],
        "outputs": [
            {
                "name": "returnData",
                "type": "tuple[]",
                "components": [
                    {"name": "success",    "type": "bool"},
                    {"name": "returnData", "type": "bytes"},
                ],
            }
        ],
        "stateMutability": "payable",
    }
]


def multicall3(w3: Web3, calls: list) -> list:
    """
    Execute N eth_call reads in a single RPC round-trip via Multicall3.

    Parameters
    ----------
    w3 : Web3
        Read provider (dRPC).  Never pass an Alchemy w3 here.
    calls : list[{"target": str, "callData": bytes | HexStr}]
        Each entry describes one subcall.

    Returns
    -------
    list[bytes | None]
        Raw ABI-encoded return bytes for each subcall, in input order.
        None when a subcall reverted (allowFailure=True is always set).
    """
    if not calls:
        return []

    mc = w3.eth.contract(
        address=Web3.to_checksum_address(MULTICALL3_ADDRESS),
        abi=MULTICALL3_ABI,
    )
    prepared = [
        {
            "target":       c["target"],
            "allowFailure": True,
            "callData":     c["callData"],
        }
        for c in calls
    ]
    results = mc.functions.aggregate3(prepared).call()
    # web3.py may return each Result as an AttributeTuple (positional) or dict;
    # use positional access (index 0 = success, index 1 = returnData) to be safe.
    return [
        (r[1] if r[0] else None)
        for r in results
    ]
