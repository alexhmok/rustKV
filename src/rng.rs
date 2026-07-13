//! Small deterministic PRNG (SplitMix64, Vigna's reference algorithm).
//!
//! Hand-rolled because `rand` is not on the runtime dependency whitelist and
//! seeded randomness is a product feature here: the simulated transport (and
//! later, election timeouts) must be reproducible from a seed. NOT
//! cryptographic — never use for anything security-relevant.

use std::ops::RangeInclusive;

#[derive(Debug, Clone)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform draw from an inclusive range. Uses a modulo reduction; the
    /// bias is negligible for the small ranges used in fault injection.
    pub fn next_range(&mut self, range: RangeInclusive<u64>) -> u64 {
        let (lo, hi) = range.into_inner();
        assert!(lo <= hi, "next_range: empty range {lo}..={hi}");
        let span = (hi - lo).wrapping_add(1);
        if span == 0 {
            // lo..=hi covers all of u64.
            return self.next_u64();
        }
        lo + self.next_u64() % span
    }

    /// True with probability `p` (values outside [0, 1] saturate).
    pub fn next_bool(&mut self, p: f64) -> bool {
        // Top 53 bits → uniform float in [0, 1).
        let unit = (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64;
        unit < p
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_reference_splitmix64_vectors() {
        // Reference outputs of Vigna's splitmix64.c for seeds 0 and 42.
        let mut rng = SplitMix64::new(0);
        assert_eq!(rng.next_u64(), 0xE220_A839_7B1D_CDAF);
        assert_eq!(rng.next_u64(), 0x6E78_9E6A_A1B9_65F4);
        assert_eq!(rng.next_u64(), 0x06C4_5D18_8009_454F);

        let mut rng = SplitMix64::new(42);
        assert_eq!(rng.next_u64(), 0xBDD7_3226_2FEB_6E95);
        assert_eq!(rng.next_u64(), 0x28EF_E333_B266_F103);
        assert_eq!(rng.next_u64(), 0x4752_6757_130F_9F52);
    }

    #[test]
    fn same_seed_same_sequence() {
        let mut a = SplitMix64::new(7);
        let mut b = SplitMix64::new(7);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn next_range_stays_in_bounds() {
        let mut rng = SplitMix64::new(1);
        for _ in 0..1000 {
            let v = rng.next_range(10..=20);
            assert!((10..=20).contains(&v));
        }
        assert_eq!(rng.next_range(5..=5), 5);
        // Full-u64 range must not panic on the span overflow.
        let _ = rng.next_range(0..=u64::MAX);
    }

    #[test]
    fn next_bool_extremes() {
        let mut rng = SplitMix64::new(2);
        for _ in 0..100 {
            assert!(!rng.next_bool(0.0));
            assert!(rng.next_bool(1.0));
        }
    }
}
