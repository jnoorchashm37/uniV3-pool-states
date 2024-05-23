use alloy_primitives::{Address, U160, U256};
use malachite::Natural;

#[derive(Debug, Clone, PartialEq)]
pub struct TokenInfo {
    pub address: Address,
    pub decimals: u8,
}

impl TokenInfo {
    pub fn new(address: Address, decimals: u8) -> Self {
        Self { address, decimals }
    }
}

pub fn u160_to_natural(num: U160) -> Natural {
    Natural::from_limbs_asc(&num.into_limbs())
}

pub fn u256_to_natural(num: U256) -> Natural {
    Natural::from_limbs_asc(&num.into_limbs())
}

pub mod serde_u256 {
    use alloy_primitives::U256;
    use serde::{
        de::{Deserialize, Deserializer},
        ser::{Serialize, Serializer},
    };

    pub fn serialize<S: Serializer>(u: &U256, serializer: S) -> Result<S::Ok, S::Error> {
        let bytes: [u8; 32] = u.to_le_bytes();
        bytes.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<U256, D::Error>
    where
        D: Deserializer<'de>,
    {
        let u: [u8; 32] = Deserialize::deserialize(deserializer)?;
        Ok(U256::from_le_bytes(u))
    }
}

pub mod serde_u160 {
    use alloy_primitives::U160;
    use serde::{
        de::{Deserialize, Deserializer},
        ser::{Serialize, Serializer},
    };

    pub fn serialize<S: Serializer>(u: &U160, serializer: S) -> Result<S::Ok, S::Error> {
        let bytes: [u8; 32] = u.to_le_bytes();
        bytes.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<U160, D::Error>
    where
        D: Deserializer<'de>,
    {
        let u: [u8; 32] = Deserialize::deserialize(deserializer)?;
        Ok(U160::from_le_bytes(u))
    }
}

pub mod serde_address {
    use std::str::FromStr;

    use alloy_primitives::Address;

    use serde::{
        de::{Deserialize, Deserializer},
        ser::Serializer,
        Serialize,
    };

    pub fn serialize<S: Serializer>(u: &Address, serializer: S) -> Result<S::Ok, S::Error> {
        format!("{:?}", u).to_lowercase().serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Address, D::Error>
    where
        D: Deserializer<'de>,
    {
        let u: String = Deserialize::deserialize(deserializer)?;
        println!("VAL: {:?}", u);
        Address::from_str(&u).map_err(serde::de::Error::custom)
    }
}

pub mod serde_tx_hash {
    use std::str::FromStr;

    use alloy_primitives::TxHash;

    use serde::{
        de::{Deserialize, Deserializer},
        ser::Serializer,
        Serialize,
    };

    pub fn serialize<S: Serializer>(u: &TxHash, serializer: S) -> Result<S::Ok, S::Error> {
        let s = format!("{:?}", u).to_lowercase();

        s.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<TxHash, D::Error>
    where
        D: Deserializer<'de>,
    {
        let u: String = Deserialize::deserialize(deserializer)?;

        TxHash::from_str(&u).map_err(serde::de::Error::custom)
    }
}
