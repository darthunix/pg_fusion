use std::alloc::{Layout, LayoutError};
use std::error::Error;
use std::fmt;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::RuntimeFilterHeader;

const HASH_GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;
const HASH_SALT: u64 = 0xD1B5_4A32_D192_ED03;

/// Bloom filter sizing and hash seed.
///
/// The fields are intentionally private so every value attached to safe APIs
/// satisfies the invariants required by [`AtomicBloomRef`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BloomParams {
    bit_count: usize,
    word_count: usize,
    hash_count: usize,
    seed: u64,
}

impl BloomParams {
    /// Create explicit Bloom parameters.
    ///
    /// `bit_count` is rounded up to an internal `u64` word count. `hash_count`
    /// is the number of bit positions set or checked for each input hash.
    pub fn new(bit_count: usize, hash_count: usize, seed: u64) -> Result<Self, BloomParamError> {
        if bit_count == 0 {
            return Err(BloomParamError::ZeroBitCount);
        }
        if hash_count == 0 {
            return Err(BloomParamError::ZeroHashCount);
        }

        let word_count = bit_count
            .checked_add(63)
            .ok_or(BloomParamError::TooManyBits)?
            / 64;

        Ok(Self {
            bit_count,
            word_count,
            hash_count,
            seed,
        })
    }

    /// Estimate Bloom parameters for an expected cardinality and false-positive
    /// rate.
    pub fn for_expected_items(
        expected_items: usize,
        false_positive_rate: f64,
        seed: u64,
    ) -> Result<Self, BloomParamError> {
        if expected_items == 0 {
            return Err(BloomParamError::ZeroExpectedItems);
        }
        if !false_positive_rate.is_finite()
            || false_positive_rate <= 0.0
            || false_positive_rate >= 1.0
        {
            return Err(BloomParamError::InvalidFalsePositiveRate(
                false_positive_rate,
            ));
        }

        let expected = expected_items as f64;
        let ln2 = std::f64::consts::LN_2;
        let bit_count_f = (-(expected * false_positive_rate.ln()) / (ln2 * ln2)).ceil();
        if bit_count_f > usize::MAX as f64 {
            return Err(BloomParamError::TooManyBits);
        }
        let bit_count = bit_count_f as usize;
        let hash_count = ((bit_count as f64 / expected) * ln2).round().max(1.0) as usize;

        Self::new(bit_count, hash_count, seed)
    }

    /// Number of addressable Bloom bits.
    pub fn bit_count(self) -> usize {
        self.bit_count
    }

    /// Number of [`AtomicU64`] words needed to store the bitset.
    pub fn word_count(self) -> usize {
        self.word_count
    }

    /// Number of derived bit positions used per key.
    pub fn hash_count(self) -> usize {
        self.hash_count
    }

    /// Seed mixed into every inserted or probed hash.
    pub fn seed(self) -> u64 {
        self.seed
    }
}

/// Invalid Bloom sizing input.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum BloomParamError {
    ZeroBitCount,
    ZeroHashCount,
    ZeroExpectedItems,
    InvalidFalsePositiveRate(f64),
    TooManyBits,
}

impl fmt::Display for BloomParamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroBitCount => f.write_str("bloom filter bit count must be greater than zero"),
            Self::ZeroHashCount => f.write_str("bloom filter hash count must be greater than zero"),
            Self::ZeroExpectedItems => f.write_str("expected item count must be greater than zero"),
            Self::InvalidFalsePositiveRate(rate) => {
                write!(
                    f,
                    "false positive rate must be finite and in (0, 1), got {rate}"
                )
            }
            Self::TooManyBits => f.write_str("bloom filter bit count exceeds addressable memory"),
        }
    }
}

impl Error for BloomParamError {}

/// Failure to attach a Bloom view to caller-owned storage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BloomAttachError {
    NullBits,
    InsufficientWords { required: usize, actual: usize },
}

impl fmt::Display for BloomAttachError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NullBits => f.write_str("bloom filter bit pointer is null"),
            Self::InsufficientWords { required, actual } => write!(
                f,
                "bloom filter storage has {actual} words, but {required} are required",
            ),
        }
    }
}

impl Error for BloomAttachError {}

/// Atomic Bloom filter over caller-owned [`AtomicU64`] storage.
///
/// This type does not own the memory and does not encode lifecycle state. It is
/// safe to share between processes or threads as long as all participants agree
/// on the same [`BloomParams`] and external lifecycle rules prevent clearing
/// while old probes can still rely on the bits.
#[derive(Clone, Copy, Debug)]
pub struct AtomicBloomRef<'a> {
    bits: &'a [AtomicU64],
    params: BloomParams,
}

impl<'a> AtomicBloomRef<'a> {
    /// Attach to a slice of initialized atomic words.
    pub fn new(bits: &'a [AtomicU64], params: BloomParams) -> Result<Self, BloomAttachError> {
        if bits.len() < params.word_count {
            return Err(BloomAttachError::InsufficientWords {
                required: params.word_count,
                actual: bits.len(),
            });
        }
        Ok(Self {
            bits: &bits[..params.word_count],
            params,
        })
    }

    /// Attach to caller-owned atomic bit storage.
    ///
    /// # Safety
    ///
    /// `bits_ptr` must point to `word_count` initialized, properly aligned
    /// `AtomicU64` values that remain valid for the returned lifetime.
    pub unsafe fn from_raw_parts(
        bits_ptr: *const AtomicU64,
        word_count: usize,
        params: BloomParams,
    ) -> Result<Self, BloomAttachError> {
        let bits_ptr =
            NonNull::new(bits_ptr as *mut AtomicU64).ok_or(BloomAttachError::NullBits)?;
        let bits = std::slice::from_raw_parts(bits_ptr.as_ptr(), word_count);
        Self::new(bits, params)
    }

    /// Return the parameters used by this Bloom view.
    pub fn params(&self) -> BloomParams {
        self.params
    }

    /// Clear all Bloom words.
    ///
    /// Callers must provide lifecycle synchronization. Clearing a ready filter
    /// while an old probe can still read it can create false negatives.
    pub fn clear(&self) {
        for word in self.bits {
            word.store(0, Ordering::Relaxed);
        }
    }

    /// Insert an integer value as an already-normalized key.
    pub fn insert_u64(&self, value: u64) {
        self.insert_hash(value);
    }

    /// Check an integer value as an already-normalized key.
    pub fn might_contain_u64(&self, value: u64) -> bool {
        self.might_contain_hash(value)
    }

    /// Insert an already-hashed key.
    pub fn insert_hash(&self, hash: u64) {
        for i in 0..self.params.hash_count {
            let (word_index, mask) = self.word_mask(hash, i);
            self.bits[word_index].fetch_or(mask, Ordering::Relaxed);
        }
    }

    /// Return whether an already-hashed key may be present.
    ///
    /// `false` means definitely absent for the current Bloom contents. `true`
    /// may be a true positive or a Bloom false positive.
    pub fn might_contain_hash(&self, hash: u64) -> bool {
        for i in 0..self.params.hash_count {
            let (word_index, mask) = self.word_mask(hash, i);
            if self.bits[word_index].load(Ordering::Relaxed) & mask == 0 {
                return false;
            }
        }
        true
    }

    #[inline]
    fn word_mask(&self, hash: u64, hash_index: usize) -> (usize, u64) {
        let bit = self.bit_index(hash, hash_index);
        (bit / 64, 1u64 << (bit % 64))
    }

    #[inline]
    fn bit_index(&self, hash: u64, hash_index: usize) -> usize {
        let h1 = splitmix64(hash ^ self.params.seed);
        let h2 = splitmix64(h1 ^ HASH_SALT) | 1;
        let value = h1.wrapping_add((hash_index as u64).wrapping_mul(h2));
        (value % self.params.bit_count as u64) as usize
    }
}

/// Layout for a standalone [`RuntimeFilterHeader`] plus Bloom bitset.
#[derive(Clone, Copy, Debug)]
pub struct RuntimeFilterLayout {
    pub layout: Layout,
    pub bits_offset: usize,
    pub word_count: usize,
}

/// Compute a C-compatible memory layout for a standalone runtime-filter slot.
pub fn runtime_filter_layout(params: BloomParams) -> Result<RuntimeFilterLayout, LayoutError> {
    let header = Layout::new::<RuntimeFilterHeader>();
    let bits = Layout::array::<AtomicU64>(params.word_count)?;
    let (layout, bits_offset) = header.extend(bits)?;
    Ok(RuntimeFilterLayout {
        layout: layout.pad_to_align(),
        bits_offset,
        word_count: params.word_count,
    })
}

/// Return typed pointers into a region described by [`runtime_filter_layout`].
///
/// # Safety
///
/// `base` must point to an allocation at least `layout.layout.size()` bytes
/// long with `layout.layout.align()` alignment.
pub unsafe fn runtime_filter_ptrs(
    base: *mut u8,
    layout: RuntimeFilterLayout,
) -> (*mut RuntimeFilterHeader, *mut AtomicU64) {
    let header_ptr = base.cast::<RuntimeFilterHeader>();
    let bits_ptr = base.add(layout.bits_offset).cast::<AtomicU64>();
    (header_ptr, bits_ptr)
}

#[inline]
fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(HASH_GAMMA);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}
