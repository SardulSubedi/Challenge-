use std::collections::BTreeMap;

use anyhow::{Result, ensure};
use chrono::{DateTime, Utc};

use crate::{
    Side,
    util::dtos::challenge_orderbook::{
        SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
    },
};

pub const SCALE: i64 = 1_000_000;

/// Exact decimal conversion. Never round financial values through binary floats.
pub fn fixed(value: &str) -> Result<i64> {
    let (negative, unsigned) = match value.strip_prefix('-') {
        Some(v) => (true, v),
        None => (false, value),
    };
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    ensure!(!whole.is_empty(), "invalid decimal");
    let mut integral = 0u64;
    for c in whole.bytes() {
        ensure!(c.is_ascii_digit(), "invalid decimal");
        integral = integral
            .checked_mul(10)
            .and_then(|v| v.checked_add(u64::from(c - b'0')))
            .ok_or_else(|| anyhow::anyhow!("decimal overflow"))?;
    }
    let mut fractional = 0u64;
    for (i, c) in fraction.bytes().enumerate() {
        ensure!(c.is_ascii_digit(), "invalid decimal fraction");
        if i < 6 {
            fractional = fractional * 10 + u64::from(c - b'0');
        } else {
            ensure!(c == b'0', "decimal exceeds six-place schema precision");
        }
    }
    const POW10: [u64; 7] = [1, 10, 100, 1000, 10000, 100000, 1000000];
    fractional *= POW10[6 - fraction.len().min(6)];
    let magnitude = integral
        .checked_mul(SCALE as u64)
        .and_then(|v| v.checked_add(fractional))
        .ok_or_else(|| anyhow::anyhow!("decimal overflow"))?;
    if negative && magnitude == (1u64 << 63) {
        return Ok(i64::MIN);
    }
    let magnitude = i64::try_from(magnitude)?;
    Ok(if negative { -magnitude } else { magnitude })
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Book {
    pub bids: BTreeMap<i64, i64>,
    pub asks: BTreeMap<i64, i64>,
}

impl Book {
    pub fn from_levels(bids: Vec<(i64, i64)>, asks: Vec<(i64, i64)>) -> Result<Self> {
        let mut book = Self::default();
        for (side, mut levels) in [(Side::Buy, bids), (Side::Sell, asks)] {
            for &(price, quantity) in &levels {
                ensure!(quantity >= 0, "negative snapshot quantity");
                ensure!((0..=SCALE).contains(&price), "price outside [0,1]");
            }
            levels.retain(|(_, quantity)| *quantity > 0);
            levels.sort_unstable_by_key(|&(price, _)| price);
            ensure!(
                levels.windows(2).all(|pair| pair[0].0 != pair[1].0),
                "duplicate snapshot price"
            );
            // Bulk construction fills compact tree nodes instead of repeatedly
            // searching/splitting while inserting the initial snapshot.
            *book.levels_mut(side) = levels.into_iter().collect();
        }
        Ok(book)
    }

    fn levels_mut(&mut self, side: Side) -> &mut BTreeMap<i64, i64> {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    /// Returns the absolute new quantity, including zero for deletion.
    pub fn apply(&mut self, side: Side, price: i64, delta: i64) -> Result<i64> {
        ensure!((0..=SCALE).contains(&price), "price outside [0,1]");
        let levels = self.levels_mut(side);
        use std::collections::btree_map::Entry;
        // One tree traversal, including deletion. Validate before mutating an entry.
        let quantity = match levels.entry(price) {
            Entry::Occupied(mut entry) => {
                let quantity = entry
                    .get()
                    .checked_add(delta)
                    .ok_or_else(|| anyhow::anyhow!("quantity overflow"))?;
                ensure!(
                    quantity >= 0,
                    "delta creates negative quantity; snapshot required"
                );
                if quantity == 0 {
                    entry.remove();
                } else {
                    *entry.get_mut() = quantity;
                }
                quantity
            }
            Entry::Vacant(entry) => {
                ensure!(
                    delta >= 0,
                    "delta creates negative quantity; snapshot required"
                );
                if delta > 0 {
                    entry.insert(delta);
                }
                delta
            }
        };
        Ok(quantity)
    }

    pub fn snapshot(
        &self,
        symbol: &str,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
    ) -> SnapshotDomain {
        SnapshotDomain {
            received_at,
            exchange_timestamp,
            symbol: symbol.to_owned(),
            bids: self
                .bids
                .iter()
                .rev()
                .map(|(&price_level, &quantity)| SnapshotBidsEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            asks: self
                .asks
                .iter()
                .map(|(&price_level, &quantity)| SnapshotAsksEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn decimal_is_exact_and_rejects_precision_loss() {
        assert_eq!(fixed("0.123456").unwrap(), 123456);
        assert_eq!(fixed("-54.00").unwrap(), -54_000_000);
        assert_eq!(fixed("1.0000000").unwrap(), SCALE);
        assert!(fixed("0.1234567").is_err());
        assert!(fixed("NaN").is_err());
        assert!(fixed("9223372036854775807").is_err());
    }
    #[test]
    fn decimal_boundaries_match_independent_i128_scaling() {
        assert_eq!(fixed("9223372036854.775807").unwrap(), i64::MAX);
        assert_eq!(fixed("-9223372036854.775808").unwrap(), i64::MIN);
        for bad in [
            "9223372036854.775808",
            "-9223372036854.775809",
            "18446744073709551616",
            "-",
            ".1",
            "1..0",
            "1.0000001",
            "+1",
            "1e3",
            " 1",
        ] {
            assert!(fixed(bad).is_err(), "{bad}");
        }
        for whole in [-9223372036854i128, -100, 0, 1, 9223372036854] {
            for fraction in [0i128, 1, 10, 99999, 775807, 775808, 999999] {
                let negative = whole < 0;
                let text = format!(
                    "{}{}.{fraction:06}000",
                    if negative { "-" } else { "" },
                    whole.abs()
                );
                let magnitude = whole.abs() * 1_000_000 + fraction;
                let expected = i64::try_from(if negative { -magnitude } else { magnitude });
                assert_eq!(fixed(&text).ok(), expected.ok(), "{text}");
            }
        }
    }
    #[test]
    fn entry_update_failures_leave_existing_and_absent_levels_unchanged() {
        let mut b = Book::from_levels(vec![(100, i64::MAX)], vec![]).unwrap();
        let original = b.clone();
        assert!(b.apply(Side::Buy, 100, 1).is_err());
        assert!(b.apply(Side::Buy, 200, -1).is_err());
        assert_eq!(b, original);
        assert_eq!(b.apply(Side::Buy, 200, 0).unwrap(), 0);
        assert_eq!(b, original);
    }
    #[test]
    fn updates_are_absolute_and_underflow_does_not_mutate() {
        let mut b = Book::from_levels(vec![(400000, 2000000)], vec![]).unwrap();
        assert_eq!(b.apply(Side::Buy, 400000, -750000).unwrap(), 1250000);
        assert!(b.apply(Side::Buy, 400000, -2000000).is_err());
        assert_eq!(b.bids[&400000], 1250000);
        assert_eq!(b.apply(Side::Buy, 400000, -1250000).unwrap(), 0);
        assert!(b.bids.is_empty());
    }
}
