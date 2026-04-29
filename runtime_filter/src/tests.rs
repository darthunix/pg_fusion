use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};

use super::*;

fn bit_storage(words: usize) -> Vec<AtomicU64> {
    (0..words).map(|_| AtomicU64::new(0)).collect()
}

fn slot_fixture(params: BloomParams) -> (RuntimeFilterHeader, Vec<AtomicU64>, BloomParams) {
    let bits = bit_storage(params.word_count());
    (RuntimeFilterHeader::free(), bits, params)
}

struct PoolMemory {
    ptr: NonNull<u8>,
    layout: Layout,
}

impl Drop for PoolMemory {
    fn drop(&mut self) {
        unsafe { dealloc(self.ptr.as_ptr(), self.layout) };
    }
}

fn pool_fixture(slot_count: u32) -> (RuntimeFilterPool, PoolMemory) {
    let params = BloomParams::new(1024, 3, 17).unwrap();
    let config = RuntimeFilterPoolConfig::new(slot_count, params);
    let pool_layout = RuntimeFilterPool::layout(config).unwrap();
    let layout = Layout::from_size_align(pool_layout.size, pool_layout.align).unwrap();
    let ptr = NonNull::new(unsafe { alloc_zeroed(layout) }).expect("pool allocation");
    let memory = PoolMemory { ptr, layout };
    let pool = unsafe { RuntimeFilterPool::init_in_place(ptr.as_ptr(), pool_layout.size, config) }
        .expect("pool init");
    (pool, memory)
}

fn expect_builder_error(
    result: Result<RuntimeFilterBuilder<'_>, LifecycleError>,
) -> LifecycleError {
    match result {
        Ok(_) => panic!("builder acquisition unexpectedly succeeded"),
        Err(error) => error,
    }
}

#[test]
fn atomic_bloom_has_no_false_negatives_for_inserted_keys() {
    let params = BloomParams::for_expected_items(1_000, 0.01, 0xA5A5).unwrap();
    let bits = bit_storage(params.word_count());
    let bloom = AtomicBloomRef::new(&bits, params).unwrap();

    for key in 0..1_000u64 {
        bloom.insert_u64(key);
    }

    for key in 0..1_000u64 {
        assert!(bloom.might_contain_u64(key));
    }
}

#[test]
fn builder_lease_publishes_ready_filter() {
    let params = BloomParams::new(512, 4, 42).unwrap();
    let (header, bits, params) = slot_fixture(params);
    let slot = RuntimeFilterSlot::new(&header, &bits, params).unwrap();

    let builder = slot.try_acquire_builder().unwrap();
    assert_eq!(builder.generation(), 1);
    builder.insert_u64(10);
    let probe = builder.publish_ready().unwrap();

    assert_eq!(
        slot.snapshot(),
        LifecycleSnapshot {
            generation: 1,
            state: RuntimeFilterState::Ready
        }
    );
    assert_eq!(probe.decision_for_u64(10), ProbeDecision::MaybePresent);
    assert_eq!(probe.decision_for_u64(99), ProbeDecision::DefinitelyAbsent);
}

#[test]
fn free_building_disabled_and_stale_probe_generations_never_reject() {
    let params = BloomParams::new(256, 3, 0).unwrap();
    let (header, bits, params) = slot_fixture(params);
    let slot = RuntimeFilterSlot::new(&header, &bits, params).unwrap();

    assert_eq!(
        slot.probe(1).decision_for_u64(99),
        ProbeDecision::PassUnfiltered
    );

    {
        let builder = slot.try_acquire_builder().unwrap();
        builder.insert_u64(1);
        assert_eq!(
            slot.probe(builder.generation()).decision_for_u64(99),
            ProbeDecision::PassUnfiltered
        );
    }

    assert_eq!(
        slot.snapshot(),
        LifecycleSnapshot {
            generation: 1,
            state: RuntimeFilterState::Disabled
        }
    );
    assert_eq!(
        slot.probe(1).decision_for_u64(99),
        ProbeDecision::PassUnfiltered
    );

    let builder = slot.try_acquire_builder().unwrap();
    builder.insert_u64(2);
    let probe = builder.publish_ready().unwrap();
    assert_eq!(probe.generation(), 2);
    assert_eq!(
        slot.probe(1).decision_for_u64(2),
        ProbeDecision::PassUnfiltered
    );
}

#[test]
fn second_builder_is_rejected_while_one_builder_owns_payload() {
    let params = BloomParams::new(256, 3, 0).unwrap();
    let (header, bits, params) = slot_fixture(params);
    let slot = RuntimeFilterSlot::new(&header, &bits, params).unwrap();

    let builder = slot.try_acquire_builder().unwrap();
    assert_eq!(
        expect_builder_error(slot.try_acquire_builder()),
        LifecycleError::Busy {
            snapshot: LifecycleSnapshot {
                generation: 1,
                state: RuntimeFilterState::Building
            }
        }
    );

    builder.insert_u64(7);
    let probe = builder.publish_ready().unwrap();
    assert_eq!(probe.decision_for_u64(7), ProbeDecision::MaybePresent);
}

#[test]
fn ready_slot_is_not_reused_without_cleanup() {
    let params = BloomParams::new(256, 3, 0).unwrap();
    let (header, bits, params) = slot_fixture(params);
    let slot = RuntimeFilterSlot::new(&header, &bits, params).unwrap();

    let builder = slot.try_acquire_builder().unwrap();
    builder.insert_u64(7);
    let probe = builder.publish_ready().unwrap();

    assert_eq!(
        expect_builder_error(slot.try_acquire_builder()),
        LifecycleError::Busy {
            snapshot: LifecycleSnapshot {
                generation: 1,
                state: RuntimeFilterState::Ready
            }
        }
    );
    assert_eq!(probe.decision_for_u64(7), ProbeDecision::MaybePresent);
}

#[test]
fn quiescent_retire_allows_ready_slot_reuse() {
    let params = BloomParams::new(256, 3, 0).unwrap();
    let (header, bits, params) = slot_fixture(params);
    let slot = RuntimeFilterSlot::new(&header, &bits, params).unwrap();

    let builder = slot.try_acquire_builder().unwrap();
    builder.insert_u64(7);
    let old_probe = builder.publish_ready().unwrap();
    assert_eq!(old_probe.decision_for_u64(7), ProbeDecision::MaybePresent);

    unsafe {
        slot.retire_ready_after_quiescence(old_probe.generation())
            .unwrap();
    }
    assert_eq!(old_probe.decision_for_u64(7), ProbeDecision::PassUnfiltered);

    let builder = slot.try_acquire_builder().unwrap();
    assert_eq!(builder.generation(), 2);
    assert_eq!(old_probe.decision_for_u64(7), ProbeDecision::PassUnfiltered);
    builder.insert_u64(9);
    let new_probe = builder.publish_ready().unwrap();
    assert_eq!(new_probe.decision_for_u64(9), ProbeDecision::MaybePresent);
}

#[test]
fn stale_ready_retire_cannot_disable_newer_generation() {
    let params = BloomParams::new(256, 3, 0).unwrap();
    let (header, bits, params) = slot_fixture(params);
    let slot = RuntimeFilterSlot::new(&header, &bits, params).unwrap();

    let builder = slot.try_acquire_builder().unwrap();
    let probe = builder.publish_ready().unwrap();

    assert_eq!(
        unsafe { slot.retire_ready_after_quiescence(0).unwrap_err() },
        LifecycleError::InvalidTransition {
            expected: LifecycleSnapshot {
                generation: 0,
                state: RuntimeFilterState::Ready
            },
            actual: LifecycleSnapshot {
                generation: 1,
                state: RuntimeFilterState::Ready
            }
        }
    );

    unsafe {
        slot.retire_ready_after_quiescence(probe.generation())
            .unwrap();
    }
    let builder = slot.try_acquire_builder().unwrap();
    let new_probe = builder.publish_ready().unwrap();
    assert_eq!(new_probe.generation(), 2);

    assert_eq!(
        unsafe { slot.retire_ready_after_quiescence(1).unwrap_err() },
        LifecycleError::InvalidTransition {
            expected: LifecycleSnapshot {
                generation: 1,
                state: RuntimeFilterState::Ready
            },
            actual: LifecycleSnapshot {
                generation: 2,
                state: RuntimeFilterState::Ready
            }
        }
    );
}

#[test]
fn dropped_builder_disables_and_later_builder_gets_new_generation() {
    let params = BloomParams::new(256, 3, 0).unwrap();
    let (header, bits, params) = slot_fixture(params);
    let slot = RuntimeFilterSlot::new(&header, &bits, params).unwrap();

    {
        let builder = slot.try_acquire_builder().unwrap();
        builder.insert_u64(7);
    }
    assert_eq!(
        slot.snapshot(),
        LifecycleSnapshot {
            generation: 1,
            state: RuntimeFilterState::Disabled
        }
    );

    let builder = slot.try_acquire_builder().unwrap();
    assert_eq!(builder.generation(), 2);
    builder.insert_u64(9);
    let probe = builder.publish_ready().unwrap();
    assert_eq!(probe.decision_for_u64(9), ProbeDecision::MaybePresent);
    assert_eq!(
        slot.probe(1).decision_for_u64(7),
        ProbeDecision::PassUnfiltered
    );
}

#[test]
fn explicit_disable_allows_rebuild_without_exposing_partial_payload() {
    let params = BloomParams::new(256, 3, 0).unwrap();
    let (header, bits, params) = slot_fixture(params);
    let slot = RuntimeFilterSlot::new(&header, &bits, params).unwrap();

    let builder = slot.try_acquire_builder().unwrap();
    builder.insert_u64(7);
    builder.disable().unwrap();
    assert_eq!(
        slot.probe(1).decision_for_u64(7),
        ProbeDecision::PassUnfiltered
    );

    let builder = slot.try_acquire_builder().unwrap();
    assert_eq!(builder.generation(), 2);
}

#[test]
fn stale_builder_transition_cannot_overwrite_newer_generation() {
    let params = BloomParams::new(256, 3, 0).unwrap();
    let (header, bits, params) = slot_fixture(params);
    let slot = RuntimeFilterSlot::new(&header, &bits, params).unwrap();

    let builder = slot.try_acquire_builder().unwrap();
    header.lifecycle_word().store(
        pack_lifecycle_word(2, RuntimeFilterState::Ready).unwrap(),
        Ordering::Release,
    );

    assert_eq!(
        builder.disable().unwrap_err(),
        LifecycleError::InvalidTransition {
            expected: LifecycleSnapshot {
                generation: 1,
                state: RuntimeFilterState::Building
            },
            actual: LifecycleSnapshot {
                generation: 2,
                state: RuntimeFilterState::Ready
            }
        }
    );
    assert_eq!(
        slot.snapshot(),
        LifecycleSnapshot {
            generation: 2,
            state: RuntimeFilterState::Ready
        }
    );
}

#[test]
fn max_generation_free_slot_is_exhausted() {
    let params = BloomParams::new(64, 2, 0).unwrap();
    let (header, bits, params) = slot_fixture(params);
    let slot = RuntimeFilterSlot::new(&header, &bits, params).unwrap();
    let max_generation = u64::MAX >> 2;
    header.lifecycle_word().store(
        pack_lifecycle_word(max_generation, RuntimeFilterState::Free).unwrap(),
        Ordering::Release,
    );

    assert_eq!(
        expect_builder_error(slot.try_acquire_builder()),
        LifecycleError::GenerationExhausted {
            generation: max_generation
        }
    );
}

#[test]
fn tiny_filter_boundaries_do_not_access_past_storage() {
    let params = BloomParams::new(1, 8, u64::MAX).unwrap();
    let bits = bit_storage(params.word_count());
    let bloom = AtomicBloomRef::new(&bits, params).unwrap();

    bloom.clear();
    bloom.insert_u64(123);
    assert!(bloom.might_contain_u64(123));
    assert_eq!(params.word_count(), 1);
}

#[test]
fn parameter_validation_rejects_invalid_inputs() {
    assert_eq!(
        BloomParams::new(0, 1, 0).unwrap_err(),
        BloomParamError::ZeroBitCount
    );
    assert_eq!(
        BloomParams::new(1, 0, 0).unwrap_err(),
        BloomParamError::ZeroHashCount
    );
    assert_eq!(
        BloomParams::for_expected_items(0, 0.01, 0).unwrap_err(),
        BloomParamError::ZeroExpectedItems
    );
    assert!(matches!(
        BloomParams::for_expected_items(10, 1.0, 0).unwrap_err(),
        BloomParamError::InvalidFalsePositiveRate(_)
    ));
}

#[test]
fn lifecycle_snapshot_roundtrips_generation_and_state() {
    let word = pack_lifecycle_word(123, RuntimeFilterState::Disabled).unwrap();
    assert_eq!(
        unpack_lifecycle_word(word),
        LifecycleSnapshot {
            generation: 123,
            state: RuntimeFilterState::Disabled
        }
    );
}

#[test]
fn attach_requires_enough_words() {
    let params = BloomParams::new(129, 3, 0).unwrap();
    let bits = bit_storage(2);
    assert_eq!(params.word_count(), 3);
    assert_eq!(
        AtomicBloomRef::new(&bits, params).unwrap_err(),
        BloomAttachError::InsufficientWords {
            required: 3,
            actual: 2
        }
    );
}

#[test]
fn statistical_false_positive_rate_stays_reasonable() {
    let params = BloomParams::for_expected_items(1_000, 0.01, 0xB10F).unwrap();
    let bits = bit_storage(params.word_count());
    let bloom = AtomicBloomRef::new(&bits, params).unwrap();

    for key in 0..1_000u64 {
        bloom.insert_u64(key);
    }

    let mut false_positives = 0usize;
    let absent = 10_000usize;
    for key in 10_000..(10_000 + absent as u64) {
        if bloom.might_contain_u64(key) {
            false_positives += 1;
        }
    }

    assert!(
        false_positives < 400,
        "false positive count {false_positives} is unexpectedly high",
    );
}

#[test]
fn layout_places_bits_after_header_with_atomic_alignment() {
    let params = BloomParams::new(256, 4, 0).unwrap();
    let layout = runtime_filter_layout(params).unwrap();

    assert!(layout.bits_offset >= std::mem::size_of::<RuntimeFilterHeader>());
    assert_eq!(layout.bits_offset % std::mem::align_of::<AtomicU64>(), 0);
    assert_eq!(layout.word_count, params.word_count());
}

#[test]
fn raw_attach_accepts_valid_atomic_storage() {
    let params = BloomParams::new(64, 2, 0).unwrap();
    let bits = bit_storage(params.word_count());
    let bloom = unsafe { AtomicBloomRef::from_raw_parts(bits.as_ptr(), bits.len(), params) }
        .expect("valid raw bit storage");

    bloom.insert_u64(5);
    assert!(bloom.might_contain_u64(5));
}

#[test]
fn pool_publishes_filter_and_probe_rejects_absent_keys() {
    let (pool, _memory) = pool_fixture(1);
    let target = RuntimeFilterTarget {
        session_epoch: 11,
        scan_id: 22,
        output_column: 3,
        key_type: RuntimeFilterKeyType::Int64,
    };
    let build = pool
        .allocate_build(target)
        .expect("allocate")
        .expect("available slot");
    build.insert_hash(hash_int_key(42)).unwrap();
    build.publish_ready().unwrap();

    let mut probes = Vec::new();
    pool.lookup_probes(11, 22, &mut probes);
    assert_eq!(probes.len(), 1);
    assert_eq!(probes[0].output_column(), 3);
    assert_eq!(probes[0].key_type(), RuntimeFilterKeyType::Int64);
    assert_eq!(
        probes[0].decision_for_hash(hash_int_key(42)),
        ProbeDecision::MaybePresent
    );
    assert_eq!(
        probes[0].decision_for_hash(hash_int_key(100_000)),
        ProbeDecision::DefinitelyAbsent
    );
    assert_eq!(
        probes[0].decision_for_null(),
        ProbeDecision::DefinitelyAbsent
    );
}

#[test]
fn pool_does_not_reuse_storage_until_probes_are_dropped() {
    let (pool, _memory) = pool_fixture(1);
    let target = RuntimeFilterTarget {
        session_epoch: 1,
        scan_id: 2,
        output_column: 0,
        key_type: RuntimeFilterKeyType::Int32,
    };
    let build = pool
        .allocate_build(target)
        .expect("allocate")
        .expect("available slot");
    build.insert_hash(hash_int_key(7)).unwrap();
    build.publish_ready().unwrap();

    let mut probes = Vec::new();
    pool.lookup_probes(1, 2, &mut probes);
    assert_eq!(probes.len(), 1);
    drop(build);

    let next = pool
        .allocate_build(RuntimeFilterTarget {
            session_epoch: 3,
            scan_id: 4,
            output_column: 0,
            key_type: RuntimeFilterKeyType::Int32,
        })
        .expect("allocate while old probe exists");
    assert!(next.is_none());

    drop(probes);
    assert!(pool
        .allocate_build(RuntimeFilterTarget {
            session_epoch: 3,
            scan_id: 4,
            output_column: 0,
            key_type: RuntimeFilterKeyType::Int32,
        })
        .expect("allocate after old probe exits")
        .is_some());
}
