# Uniswap V3 Pool State
Uniswap V3 pool states post each transaction that effected the pool, since its inception:
- `ticks()` -> `PoolTickInfo` (multiple objects for each initialized tick)
- `slot()` -> `PoolSlot0`
- each object is the state of the pool *AFTER* the specified `tx_hash`

Currently supports the following pools:
- ETH-USDT: 0x4e68Ccd3E89f51C3074ca5072bbAC773960dFa36
- WBTC-ETH (0.03): 0xCBCdF9626bC03E24f779434178A73a0B4bad62eD
- WBTC-ETH (0.05): 0x4585FE77225b41b697C938B018E2Ac67Ac5a20c0
- USDC-ETH (0.05): 0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640
- USDC-ETH (0.3): 0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8
- ETH-USDT (0.05): 0x11b815efB8f581194ae79006d24E0d814B7697F6
- FRAX-USDC: 0xc63B0708E2F7e69CB8A1df0e1389A98C35A76D52
- WBTC-USDC: 0x99ac8cA7087fA4A2A1FB6357269965A2014ABc35
- ETH-weETH: 0x7A415B19932c0105c82FDB6b720bb01B0CC2CAe3
- DAI-USDC: 0x5777d92f208679DB4b9778590Fa3CAB3aC9e2168
- DAI-ETH: 0xC2e9F25Be6257c210d7Adf0D4Cd6E3E881ba25f8
- LINK-ETH: 0xa6Cc3C2531FdaA6Ae1A3CA84c2855806728693e8
- PEPE-ETH: 0x11950d141EcB863F01007AdD7D1A342041227b58
- WBTC-USDT: 0x9Db9e0e53058C89e5B94e29621a205198648425B
- MKR-ETH: 0xe8c6c9227491C0a8156A0106A0204d881BB7E531


### Note
the `--max-concurrent-tasks (-m)` cli flag consumes a lot of memory when run with the default value (10-50GB for 25000 concurrent tasks), lower it if necessary