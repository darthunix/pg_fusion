use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use pgrx_pg_sys::BLCKSZ;
use std::ptr;

fn bench_memcpy_blcksz(c: &mut Criterion) {
    let size = BLCKSZ as usize;

    // Reuse the same buffers across iterations to measure just memcpy cost
    let src = vec![0xAAu8; size];
    let mut dst = vec![0u8; size];

    let mut group = c.benchmark_group("memcpy");
    group.throughput(Throughput::Bytes(size as u64));
    group.bench_function("memcpy_blcksz", |b| {
        b.iter(|| unsafe {
            let src_ptr = black_box(src.as_ptr());
            let dst_ptr = black_box(dst.as_mut_ptr());
            ptr::copy_nonoverlapping(src_ptr, dst_ptr, size);
            black_box(&dst);
        })
    });
    group.finish();
}

criterion_group!(benches, bench_memcpy_blcksz);
criterion_main!(benches);

