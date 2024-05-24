pub const INITIAL_POOLS: &str = r#"WITH 
    initial_pools AS (
        SELECT arrayJoin([
            '0x4e68ccd3e89f51c3074ca5072bbac773960dfa36',
            '0xcbcdf9626bc03e24f779434178a73a0b4bad62ed',
            '0x11b815efb8f581194ae79006d24e0d814b7697f6',
            '0xc63b0708e2f7e69cb8a1df0e1389a98c35a76d52',
            '0x99ac8ca7087fa4a2a1fb6357269965a2014abc35',
            '0x7a415b19932c0105c82fdb6b720bb01b0cc2cae3',
            '0x5777d92f208679db4b9778590fa3cab3ac9e2168',
            '0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8',
            '0xa6cc3c2531fdaa6ae1a3ca84c2855806728693e8',
            '0x11950d141ecb863f01007add7d1a342041227b58',
            '0x9db9e0e53058c89e5b94e29621a205198648425b',
            '0xe8c6c9227491c0a8156a0106a0204d881bb7e531',
            '0x4585fe77225b41b697c938b018e2ac67ac5a20c0',
            '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640',
            '0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8'
        ]) AS pool
    )
SELECT
    toString(p.address) AS pool_address,
    toString(p.tokens[1]) AS token0_address,
    CAST(t0.decimals, 'UInt8') AS token0_decimals,
    toString(p.tokens[2]) AS token1_address,
    CAST(t1.decimals, 'UInt8') AS token1_decimals,
    CAST(init_block, 'UInt64') AS creation_block
FROM ethereum.pools p
INNER JOIN initial_pools n ON n.pool = p.address
INNER JOIN ethereum.dex_tokens t0 ON token0_address = t0.address
INNER JOIN ethereum.dex_tokens t1 ON token1_address = t1.address








univ3-pool-states -t -s 19377000 -e 19377650 -i 10"#;

