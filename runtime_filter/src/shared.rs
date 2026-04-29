use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{AtomicBloomRef, BloomAttachError, BloomParams};

const STATE_BITS: u64 = 2;
const STATE_MASK: u64 = (1 << STATE_BITS) - 1;
const MAX_GENERATION: u64 = u64::MAX >> STATE_BITS;

/// Lifecycle state for one Bloom payload generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum RuntimeFilterState {
    /// No active owner; storage may be claimed by a new builder.
    Free = 0,
    /// A builder is populating the Bloom payload; probes must pass rows.
    Building = 1,
    /// The Bloom payload is complete and can reject absent probe keys.
    Ready = 2,
    /// This generation has no usable filter; probes must pass rows.
    Disabled = 3,
}

impl RuntimeFilterState {
    fn from_bits(bits: u64) -> Self {
        match bits {
            0 => Self::Free,
            1 => Self::Building,
            2 => Self::Ready,
            3 => Self::Disabled,
            _ => unreachable!("state bits are masked to two bits"),
        }
    }
}

/// Atomic lifecycle word decoded into generation and state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LifecycleSnapshot {
    /// Monotonically increasing slot generation.
    pub generation: u64,
    /// Current state for `generation`.
    pub state: RuntimeFilterState,
}

/// Decision returned by a probe.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProbeDecision {
    /// Filtering is unavailable or unsafe; caller must keep the row.
    PassUnfiltered,
    /// The key may be present. The caller must keep the row.
    MaybePresent,
    /// The key is definitely absent from a ready filter. The caller may skip.
    DefinitelyAbsent,
}

/// Lifecycle transition error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LifecycleError {
    GenerationExhausted {
        generation: u64,
    },
    Busy {
        snapshot: LifecycleSnapshot,
    },
    InvalidTransition {
        expected: LifecycleSnapshot,
        actual: LifecycleSnapshot,
    },
}

impl fmt::Display for LifecycleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GenerationExhausted { generation } => write!(
                f,
                "runtime filter generation {generation} cannot advance without overflowing",
            ),
            Self::Busy { snapshot } => {
                write!(f, "runtime filter slot is busy: {:?}", snapshot)
            }
            Self::InvalidTransition { expected, actual } => write!(
                f,
                "invalid runtime filter transition: expected {:?}, observed {:?}",
                expected, actual,
            ),
        }
    }
}

impl Error for LifecycleError {}

/// Shared-memory header storing one packed runtime-filter lifecycle word.
#[repr(C)]
pub struct RuntimeFilterHeader {
    lifecycle: AtomicU64,
}

impl RuntimeFilterHeader {
    /// Create a free lifecycle header.
    pub const fn free() -> Self {
        Self {
            lifecycle: AtomicU64::new(0),
        }
    }

    /// Alias for [`RuntimeFilterHeader::free`].
    pub const fn empty() -> Self {
        Self::free()
    }

    /// Load and decode the lifecycle word.
    pub fn load(&self, ordering: Ordering) -> LifecycleSnapshot {
        unpack_lifecycle_word(self.lifecycle.load(ordering))
    }

    #[cfg(test)]
    pub(crate) fn lifecycle_word(&self) -> &AtomicU64 {
        &self.lifecycle
    }
}

impl Default for RuntimeFilterHeader {
    fn default() -> Self {
        Self::free()
    }
}

/// A standalone runtime-filter slot over a lifecycle header and Bloom bits.
///
/// Prefer [`crate::RuntimeFilterPool`] for production shared-memory reuse. This
/// lower-level type is useful for tests and for callers that can prove their
/// own quiescence rules.
pub struct RuntimeFilterSlot<'a> {
    header: &'a RuntimeFilterHeader,
    bloom: AtomicBloomRef<'a>,
}

impl<'a> RuntimeFilterSlot<'a> {
    /// Attach a slot to caller-owned header and bit storage.
    pub fn new(
        header: &'a RuntimeFilterHeader,
        bits: &'a [AtomicU64],
        params: BloomParams,
    ) -> Result<Self, BloomAttachError> {
        Ok(Self::from_parts(header, AtomicBloomRef::new(bits, params)?))
    }

    fn from_parts(header: &'a RuntimeFilterHeader, bloom: AtomicBloomRef<'a>) -> Self {
        Self { header, bloom }
    }

    /// Return the current lifecycle snapshot.
    pub fn snapshot(&self) -> LifecycleSnapshot {
        self.header.load(Ordering::Acquire)
    }

    /// Acquire an exclusive build lease and clear the Bloom payload.
    pub fn try_acquire_builder(&self) -> Result<RuntimeFilterBuilder<'a>, LifecycleError> {
        loop {
            let current_word = self.header.lifecycle.load(Ordering::Acquire);
            let current = unpack_lifecycle_word(current_word);
            match current.state {
                RuntimeFilterState::Free | RuntimeFilterState::Disabled => {}
                RuntimeFilterState::Building | RuntimeFilterState::Ready => {
                    return Err(LifecycleError::Busy { snapshot: current });
                }
            }

            let Some(next_generation) = current.generation.checked_add(1) else {
                return Err(LifecycleError::GenerationExhausted {
                    generation: current.generation,
                });
            };
            if next_generation > MAX_GENERATION {
                return Err(LifecycleError::GenerationExhausted {
                    generation: current.generation,
                });
            }

            let desired = pack_lifecycle_word(next_generation, RuntimeFilterState::Building)
                .expect("checked generation must pack");
            if self
                .header
                .lifecycle
                .compare_exchange(current_word, desired, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.bloom.clear();
                return Ok(RuntimeFilterBuilder {
                    header: self.header,
                    bloom: self.bloom,
                    generation: next_generation,
                    active: true,
                });
            }
        }
    }

    /// Publish a currently building generation as ready.
    pub fn publish_build(&self, generation: u64) -> Result<RuntimeFilterProbe<'a>, LifecycleError> {
        self.transition_build(generation, RuntimeFilterState::Ready)?;
        Ok(RuntimeFilterProbe {
            header: self.header,
            bloom: self.bloom,
            generation,
        })
    }

    /// Disable a currently building generation.
    pub fn disable_build(&self, generation: u64) -> Result<(), LifecycleError> {
        self.transition_build(generation, RuntimeFilterState::Disabled)
    }

    /// Create a probe handle for a generation.
    pub fn probe(&self, generation: u64) -> RuntimeFilterProbe<'a> {
        RuntimeFilterProbe {
            header: self.header,
            bloom: self.bloom,
            generation,
        }
    }

    fn transition_build(
        &self,
        generation: u64,
        next: RuntimeFilterState,
    ) -> Result<(), LifecycleError> {
        transition_build(self.header, generation, next)
    }

    /// Retire a published filter so the storage can be reused by a later
    /// builder generation.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that no probe for `generation` is still inside
    /// [`RuntimeFilterProbe::decision_for_hash`]. A probe that already observed
    /// `Ready` may read the Bloom bits after this method returns; reusing and
    /// clearing the bitset concurrently with that old probe can create false
    /// negatives.
    pub unsafe fn retire_ready_after_quiescence(
        &self,
        generation: u64,
    ) -> Result<(), LifecycleError> {
        let expected = LifecycleSnapshot {
            generation,
            state: RuntimeFilterState::Ready,
        };
        let expected_word = pack_lifecycle_word(generation, RuntimeFilterState::Ready)?;
        let desired = pack_lifecycle_word(generation, RuntimeFilterState::Disabled)?;
        self.header
            .lifecycle
            .compare_exchange(expected_word, desired, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|actual_word| LifecycleError::InvalidTransition {
                expected,
                actual: unpack_lifecycle_word(actual_word),
            })
    }
}

/// Exclusive build lease for one runtime-filter generation.
///
/// Dropping an active builder disables its generation, which keeps probe-side
/// behavior conservative if the build path exits early.
pub struct RuntimeFilterBuilder<'a> {
    header: &'a RuntimeFilterHeader,
    bloom: AtomicBloomRef<'a>,
    generation: u64,
    active: bool,
}

impl<'a> RuntimeFilterBuilder<'a> {
    /// Generation owned by this builder.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Detach lifecycle ownership without publishing or disabling.
    ///
    /// This is used by [`crate::RuntimeFilterPool`] after it has acquired the
    /// build lease and wants the pool handle to own the eventual transition.
    pub fn detach(mut self) -> u64 {
        self.active = false;
        self.generation
    }

    /// Insert an integer value as an already-normalized key.
    pub fn insert_u64(&self, value: u64) {
        self.bloom.insert_u64(value);
    }

    /// Insert an already-hashed key.
    pub fn insert_hash(&self, hash: u64) {
        self.bloom.insert_hash(hash);
    }

    /// Publish this builder's generation as ready and return a matching probe.
    pub fn publish_ready(mut self) -> Result<RuntimeFilterProbe<'a>, LifecycleError> {
        self.transition(RuntimeFilterState::Ready)?;
        self.active = false;
        Ok(RuntimeFilterProbe {
            header: self.header,
            bloom: self.bloom,
            generation: self.generation,
        })
    }

    /// Disable this builder's generation.
    pub fn disable(mut self) -> Result<(), LifecycleError> {
        self.transition(RuntimeFilterState::Disabled)?;
        self.active = false;
        Ok(())
    }

    fn transition(&self, next: RuntimeFilterState) -> Result<(), LifecycleError> {
        transition_build(self.header, self.generation, next)
    }
}

impl Drop for RuntimeFilterBuilder<'_> {
    fn drop(&mut self) {
        if self.active {
            let _ = self.transition(RuntimeFilterState::Disabled);
        }
    }
}

/// Probe handle for one runtime-filter generation.
#[derive(Clone, Copy)]
pub struct RuntimeFilterProbe<'a> {
    header: &'a RuntimeFilterHeader,
    bloom: AtomicBloomRef<'a>,
    generation: u64,
}

impl<'a> RuntimeFilterProbe<'a> {
    /// Generation expected by this probe.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Probe an integer value as an already-normalized key.
    pub fn decision_for_u64(&self, value: u64) -> ProbeDecision {
        self.decision_for_hash(value)
    }

    /// Probe an already-hashed key.
    pub fn decision_for_hash(&self, hash: u64) -> ProbeDecision {
        let snapshot = self.header.load(Ordering::Acquire);
        if snapshot.generation != self.generation || snapshot.state != RuntimeFilterState::Ready {
            return ProbeDecision::PassUnfiltered;
        }

        if self.bloom.might_contain_hash(hash) {
            ProbeDecision::MaybePresent
        } else {
            ProbeDecision::DefinitelyAbsent
        }
    }

    /// Probe a null key.
    ///
    /// Nulls are never inserted into build-side runtime filters, so a matching
    /// ready generation may reject them.
    pub fn decision_for_null(&self) -> ProbeDecision {
        let snapshot = self.header.load(Ordering::Acquire);
        if snapshot.generation == self.generation && snapshot.state == RuntimeFilterState::Ready {
            ProbeDecision::DefinitelyAbsent
        } else {
            ProbeDecision::PassUnfiltered
        }
    }
}

fn transition_build(
    header: &RuntimeFilterHeader,
    generation: u64,
    next: RuntimeFilterState,
) -> Result<(), LifecycleError> {
    let expected = LifecycleSnapshot {
        generation,
        state: RuntimeFilterState::Building,
    };
    let expected_word = pack_lifecycle_word(generation, RuntimeFilterState::Building)
        .expect("builder generation must pack");
    let desired = pack_lifecycle_word(generation, next).expect("builder generation must pack");
    header
        .lifecycle
        .compare_exchange(expected_word, desired, Ordering::AcqRel, Ordering::Acquire)
        .map(|_| ())
        .map_err(|actual_word| LifecycleError::InvalidTransition {
            expected,
            actual: unpack_lifecycle_word(actual_word),
        })
}

/// Pack a generation and state into the atomic lifecycle representation.
pub fn pack_lifecycle_word(
    generation: u64,
    state: RuntimeFilterState,
) -> Result<u64, LifecycleError> {
    if generation > MAX_GENERATION {
        return Err(LifecycleError::GenerationExhausted { generation });
    }
    Ok((generation << STATE_BITS) | state as u64)
}

/// Decode an atomic lifecycle word.
pub fn unpack_lifecycle_word(word: u64) -> LifecycleSnapshot {
    LifecycleSnapshot {
        generation: word >> STATE_BITS,
        state: RuntimeFilterState::from_bits(word & STATE_MASK),
    }
}
