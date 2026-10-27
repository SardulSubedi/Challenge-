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
    ensure!(
        !whole.is_empty() && whole.bytes().all(|c| c.is_ascii_digit()),
        "invalid decimal"
    );
    ensure!(
        fraction.bytes().all(|c| c.is_ascii_digit()),
        "invalid decimal fraction"
    );
    let significant = fraction.trim_end_matches('0');
    ensure!(
        significant.len() <= 6,
        "decimal exceeds six-place schema precision"
    );
    let fractional = if significant.is_empty() {
        0
    } else {
        significant.parse::<i128>()?
    };
    let magnitude = whole
        .parse::<i128>()?
        .checked_mul(i128::from(SCALE))
        .and_then(|v| {
            fractional
                .checked_mul(10i128.pow(6 - significant.len() as u32))
                .and_then(|f| v.checked_add(f))
        })
        .ok_or_else(|| anyhow::anyhow!("decimal overflow"))?;
    Ok(i64::try_from(if negative {
        -magnitude
    } else {
        magnitude
    })?)
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Book {
    pub bids: BTreeMap<i64, i64>,
    pub asks: BTreeMap<i64, i64>,
}

impl Book {
    pub fn from_levels(bids: Vec<(i64, i64)>, asks: Vec<(i64, i64)>) -> Result<Self> {
        let mut book = Self::default();
        for (side, levels) in [(Side::Buy, bids), (Side::Sell, asks)] {
            for (price, quantity) in levels {
                ensure!(quantity >= 0, "negative snapshot quantity");
                ensure!((0..=SCALE).contains(&price), "price outside [0,1]");
                if quantity > 0 {
                    ensure!(
                        book.levels_mut(side).insert(price, quantity).is_none(),
                        "duplicate snapshot price"
                    );
                }
            }
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
        let quantity = levels
            .get(&price)
            .copied()
            .unwrap_or(0)
            .checked_add(delta)
            .ok_or_else(|| anyhow::anyhow!("quantity overflow"))?;
        ensure!(
            quantity >= 0,
            "delta creates negative quantity; snapshot required"
        );
        if quantity == 0 {
            levels.remove(&price);
        } else {
            levels.insert(price, quantity);
        }
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
    fn updates_are_absolute_and_underflow_does_not_mutate() {
        let mut b = Book::from_levels(vec![(400000, 2000000)], vec![]).unwrap();
        assert_eq!(b.apply(Side::Buy, 400000, -750000).unwrap(), 1250000);
        assert!(b.apply(Side::Buy, 400000, -2000000).is_err());
        assert_eq!(b.bids[&400000], 1250000);
        assert_eq!(b.apply(Side::Buy, 400000, -1250000).unwrap(), 0);
        assert!(b.bids.is_empty());
    }
}
