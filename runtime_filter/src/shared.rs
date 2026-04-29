use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{AtomicBloomRef, BloomAttachError, BloomParams};

const STATE_BITS: u64 = 2;
const STATE_MASK: u64 = (1 << STATE_BITS) - 1;
const MAX_GENERATION: u64 = u64::MAX >> STATE_BITS;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum RuntimeFilterState {
    Free = 0,
    Building = 1,
    Ready = 2,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LifecycleSnapshot {
    pub generation: u64,
    pub state: RuntimeFilterState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProbeDecision {
    PassUnfiltered,
    MaybePresent,
    DefinitelyAbsent,
}

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

#[repr(C)]
pub struct RuntimeFilterHeader {
    lifecycle: AtomicU64,
}

impl RuntimeFilterHeader {
    pub const fn free() -> Self {
        Self {
            lifecycle: AtomicU64::new(0),
        }
    }

    pub const fn empty() -> Self {
        Self::free()
    }

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

pub struct RuntimeFilterSlot<'a> {
    header: &'a RuntimeFilterHeader,
    bloom: AtomicBloomRef<'a>,
}

impl<'a> RuntimeFilterSlot<'a> {
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

    pub fn snapshot(&self) -> LifecycleSnapshot {
        self.header.load(Ordering::Acquire)
    }

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

    pub fn publish_build(&self, generation: u64) -> Result<RuntimeFilterProbe<'a>, LifecycleError> {
        self.transition_build(generation, RuntimeFilterState::Ready)?;
        Ok(RuntimeFilterProbe {
            header: self.header,
            bloom: self.bloom,
            generation,
        })
    }

    pub fn disable_build(&self, generation: u64) -> Result<(), LifecycleError> {
        self.transition_build(generation, RuntimeFilterState::Disabled)
    }

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

pub struct RuntimeFilterBuilder<'a> {
    header: &'a RuntimeFilterHeader,
    bloom: AtomicBloomRef<'a>,
    generation: u64,
    active: bool,
}

impl<'a> RuntimeFilterBuilder<'a> {
    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn detach(mut self) -> u64 {
        self.active = false;
        self.generation
    }

    pub fn insert_u64(&self, value: u64) {
        self.bloom.insert_u64(value);
    }

    pub fn insert_hash(&self, hash: u64) {
        self.bloom.insert_hash(hash);
    }

    pub fn publish_ready(mut self) -> Result<RuntimeFilterProbe<'a>, LifecycleError> {
        self.transition(RuntimeFilterState::Ready)?;
        self.active = false;
        Ok(RuntimeFilterProbe {
            header: self.header,
            bloom: self.bloom,
            generation: self.generation,
        })
    }

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

#[derive(Clone, Copy)]
pub struct RuntimeFilterProbe<'a> {
    header: &'a RuntimeFilterHeader,
    bloom: AtomicBloomRef<'a>,
    generation: u64,
}

impl<'a> RuntimeFilterProbe<'a> {
    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn decision_for_u64(&self, value: u64) -> ProbeDecision {
        self.decision_for_hash(value)
    }

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

pub fn pack_lifecycle_word(
    generation: u64,
    state: RuntimeFilterState,
) -> Result<u64, LifecycleError> {
    if generation > MAX_GENERATION {
        return Err(LifecycleError::GenerationExhausted { generation });
    }
    Ok((generation << STATE_BITS) | state as u64)
}

pub fn unpack_lifecycle_word(word: u64) -> LifecycleSnapshot {
    LifecycleSnapshot {
        generation: word >> STATE_BITS,
        state: RuntimeFilterState::from_bits(word & STATE_MASK),
    }
}
