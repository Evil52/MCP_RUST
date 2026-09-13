//! Canonical i128 string coefficients, with lossless legacy int64 replay.

use std::fmt;

use serde::{
    Deserializer, Serializer,
    de::{self, Visitor},
};

pub fn serialize<S: Serializer>(units: &i128, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&units.to_string())
}

pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i128, D::Error> {
    struct Coefficient;
    impl Visitor<'_> for Coefficient {
        type Value = i128;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a canonical signed i128 string or a legacy int64 coefficient")
        }

        fn visit_str<E: de::Error>(self, raw: &str) -> Result<i128, E> {
            if raw.len() > 40 {
                return Err(E::custom("invalid exact finance coefficient"));
            }
            let units = raw
                .parse::<i128>()
                .map_err(|_| E::custom("invalid exact finance coefficient"))?;
            if units.to_string() != raw {
                return Err(E::custom("noncanonical exact finance coefficient"));
            }
            Ok(units)
        }

        fn visit_i64<E: de::Error>(self, units: i64) -> Result<i128, E> {
            Ok(i128::from(units))
        }

        fn visit_u64<E: de::Error>(self, units: u64) -> Result<i128, E> {
            i64::try_from(units)
                .map(i128::from)
                .map_err(|_| E::custom("legacy exact finance coefficient exceeds int64"))
        }
    }
    deserializer.deserialize_any(Coefficient)
}
