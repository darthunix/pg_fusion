use std::sync::atomic::AtomicU64;

use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use runtime_filter::{
    AtomicBloomRef, BloomParams, ProbeDecision, RuntimeFilterHeader, RuntimeFilterSlot,
};

fn bit_storage(words: usize) -> Vec<AtomicU64> {
    (0..words).map(|_| AtomicU64::new(0)).collect()
}

fn params_for(expected_items: usize) -> BloomParams {
    BloomParams::for_expected_items(expected_items, 0.01, 0x5150_5150).unwrap()
}

fn bench_atomic_bloom(c: &mut Criterion) {
    let mut ops = c.benchmark_group("atomic_bloom_ops");
    let params = params_for(1_000);
    let bits = bit_storage(params.word_count());
    let bloom = AtomicBloomRef::new(&bits, params).unwrap();

    let mut insert_key = 0u64;
    ops.bench_function("insert_u64_q02_like", |b| {
        b.iter(|| {
            insert_key = insert_key.wrapping_add(1);
            bloom.insert_u64(black_box(insert_key));
        })
    });

    bloom.clear();
    bloom.insert_u64(42);
    ops.bench_function("contains_u64_hit", |b| {
        b.iter(|| black_box(bloom.might_contain_u64(black_box(42))))
    });
    ops.bench_function("contains_u64_miss", |b| {
        b.iter(|| black_box(bloom.might_contain_u64(black_box(9_999_999))))
    });
    ops.finish();

    let mut decision = c.benchmark_group("runtime_filter_decision");
    let header = RuntimeFilterHeader::empty();
    let bits = bit_storage(params.word_count());
    let slot = RuntimeFilterSlot::new(&header, &bits, params).unwrap();
    let builder = slot.try_acquire_builder().unwrap();
    let building_probe = slot.probe(builder.generation());
    decision.bench_function("not_ready_pass_unfiltered", |b| {
        b.iter(|| {
            assert_eq!(
                black_box(building_probe.decision_for_u64(black_box(9_999_999))),
                ProbeDecision::PassUnfiltered
            )
        })
    });
    builder.insert_u64(42);
    let probe = builder.publish_ready().unwrap();
    decision.bench_function("ready_definitely_absent", |b| {
        b.iter(|| {
            assert_eq!(
                black_box(probe.decision_for_u64(black_box(9_999_999))),
                ProbeDecision::DefinitelyAbsent
            )
        })
    });
    decision.bench_function("cached_ready_contains_miss", |b| {
        let bloom = AtomicBloomRef::new(&bits, params).unwrap();
        b.iter(|| black_box(bloom.might_contain_u64(black_box(9_999_999))))
    });
    decision.finish();

    let mut lifecycle = c.benchmark_group("runtime_filter_lifecycle");
    for expected_items in [1_000usize, 100_000, 1_000_000] {
        lifecycle.bench_with_input(
            BenchmarkId::new("begin_build_clear", expected_items),
            &expected_items,
            |b, expected_items| {
                b.iter_batched(
                    || {
                        let params = params_for(*expected_items);
                        let bits = bit_storage(params.word_count());
                        let header = RuntimeFilterHeader::empty();
                        (header, bits, params)
                    },
                    |(header, bits, params)| {
                        let slot = RuntimeFilterSlot::new(&header, &bits, params).unwrap();
                        let _builder = slot.try_acquire_builder().unwrap();
                        black_box(());
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }
    lifecycle.finish();
}

criterion_group!(benches, bench_atomic_bloom);
criterion_main!(benches);
