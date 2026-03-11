use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use pgrx_pg_sys as pg_sys;
use scan::{decode_heap_page_block, heap_attrs_from_schema, HeapPageBlock};
use std::sync::Arc;
use std::time::Instant;
use storage::heap::PgAttrMeta;

struct DecodeFixture {
    schema: SchemaRef,
    attrs: Vec<PgAttrMeta>,
    proj_indices: Option<Vec<usize>>,
    block: HeapPageBlock,
}

fn align_up(off: usize, align: usize) -> usize {
    debug_assert!(align.is_power_of_two());
    (off + align - 1) & !(align - 1)
}

fn varlena_1b_header(total_len: usize) -> u8 {
    debug_assert!(total_len > 1 && total_len < 128);
    if cfg!(target_endian = "little") {
        ((total_len as u8) << 1) | 0x01
    } else {
        (total_len as u8) | 0x80
    }
}

fn pack_itemid(off: u16, flags: u8, len: u16) -> u32 {
    if cfg!(target_endian = "little") {
        ((off as u32) & 0x7FFF) | (((flags as u32) & 0x03) << 15) | (((len as u32) & 0x7FFF) << 17)
    } else {
        (((off as u32) & 0x7FFF) << 17) | (((flags as u32) & 0x03) << 15) | ((len as u32) & 0x7FFF)
    }
}

fn make_tuple_i32(v: i32) -> Vec<u8> {
    let hdr_sz = core::mem::size_of::<pg_sys::HeapTupleHeaderData>();
    let mut tuple = vec![0u8; hdr_sz];
    let off = align_up(hdr_sz, 4);
    tuple.resize(off + 4, 0);
    tuple[off..off + 4].copy_from_slice(&v.to_ne_bytes());

    let hdr = unsafe { &mut *(tuple.as_mut_ptr() as *mut pg_sys::HeapTupleHeaderData) };
    hdr.t_infomask = 0;
    hdr.t_hoff = hdr_sz as u8;
    tuple
}

fn make_tuple_i32_text(v: i32, text: &[u8]) -> Vec<u8> {
    let hdr_sz = core::mem::size_of::<pg_sys::HeapTupleHeaderData>();
    let mut tuple = vec![0u8; hdr_sz];

    let int_off = align_up(hdr_sz, 4);
    tuple.resize(int_off + 4, 0);
    tuple[int_off..int_off + 4].copy_from_slice(&v.to_ne_bytes());

    let txt_off = align_up(int_off + 4, 4);
    let total = 1 + text.len();
    tuple.resize(txt_off + total, 0);
    tuple[txt_off] = varlena_1b_header(total);
    tuple[txt_off + 1..txt_off + total].copy_from_slice(text);

    let hdr = unsafe { &mut *(tuple.as_mut_ptr() as *mut pg_sys::HeapTupleHeaderData) };
    hdr.t_infomask = 0;
    hdr.t_hoff = hdr_sz as u8;
    tuple
}

fn build_page_with_tuples(tuples: &[Vec<u8>]) -> Vec<u8> {
    let mut page = vec![0u8; pg_sys::BLCKSZ as usize];
    let header_sz = core::mem::size_of::<pg_sys::PageHeaderData>();
    let item_sz = core::mem::size_of::<pg_sys::ItemIdData>();
    let pd_lower = header_sz + item_sz * tuples.len();
    let mut data_off = align_up(pd_lower + 64, 8);

    for (i, tuple) in tuples.iter().enumerate() {
        let len = tuple.len();
        assert!(
            data_off + len <= page.len(),
            "tuple payload does not fit into page"
        );
        page[data_off..data_off + len].copy_from_slice(tuple);
        let raw = pack_itemid(data_off as u16, pg_sys::LP_NORMAL as u8, len as u16);
        let item_ptr = unsafe { page.as_mut_ptr().add(header_sz + i * item_sz) as *mut u32 };
        unsafe { core::ptr::write(item_ptr, raw) };
        data_off = align_up(data_off + len, 2);
    }

    let hdr = unsafe { &mut *(page.as_mut_ptr() as *mut pg_sys::PageHeaderData) };
    hdr.pd_lower = pd_lower as u16;
    hdr.pd_upper = pd_lower as u16;
    hdr.pd_special = page.len() as u16;
    page
}

fn visibility_bitmap(num_offsets: usize, every_other: bool) -> Vec<u8> {
    let mut vis = vec![0u8; num_offsets.div_ceil(8)];
    if every_other {
        for i in 0..num_offsets {
            if i % 2 == 0 {
                vis[i / 8] |= 1u8 << (i % 8);
            }
        }
    } else {
        vis.fill(0xFF);
        if (num_offsets % 8) != 0 {
            let keep = num_offsets % 8;
            let mask = (1u8 << keep) - 1;
            let last = vis.len() - 1;
            vis[last] &= mask;
        }
    }
    vis
}

fn make_i32_fixture(rows: usize, every_other_visible: bool) -> DecodeFixture {
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));
    let attrs = heap_attrs_from_schema(&schema);
    let mut tuples = Vec::with_capacity(rows);
    for i in 0..rows {
        tuples.push(make_tuple_i32(i as i32));
    }
    let page = build_page_with_tuples(&tuples);
    let vis = visibility_bitmap(rows, every_other_visible);
    DecodeFixture {
        schema,
        attrs,
        proj_indices: None,
        block: HeapPageBlock {
            blkno: 0,
            num_offsets: rows as u16,
            vis_len: vis.len() as u16,
            page,
            vis,
            recv_ts: Instant::now(),
        },
    }
}

fn make_i32_text_fixture(rows: usize, text_len: usize) -> DecodeFixture {
    let schema = Arc::new(Schema::new(vec![
        Field::new("i", DataType::Int32, true),
        Field::new("t", DataType::Utf8, true),
    ]));
    let attrs = heap_attrs_from_schema(&schema);
    let text = vec![b'x'; text_len];
    let mut tuples = Vec::with_capacity(rows);
    for i in 0..rows {
        tuples.push(make_tuple_i32_text(i as i32, &text));
    }
    let page = build_page_with_tuples(&tuples);
    let vis = visibility_bitmap(rows, false);
    DecodeFixture {
        schema,
        attrs,
        proj_indices: None,
        block: HeapPageBlock {
            blkno: 0,
            num_offsets: rows as u16,
            vis_len: vis.len() as u16,
            page,
            vis,
            recv_ts: Instant::now(),
        },
    }
}

fn bench_heap_decode(c: &mut Criterion) {
    let f_i32_all = make_i32_fixture(192, false);
    let f_i32_sparse = make_i32_fixture(192, true);
    let f_i32_text = make_i32_text_fixture(96, 32);

    let mut group = c.benchmark_group("heap_decode");

    group.throughput(Throughput::Elements(f_i32_all.block.num_offsets as u64));
    group.bench_function("i32_all_visible", |b| {
        b.iter(|| {
            let (batch, decoded_rows) = decode_heap_page_block(
                black_box(&f_i32_all.block),
                &f_i32_all.schema,
                &f_i32_all.attrs,
                f_i32_all.proj_indices.as_deref(),
            )
            .expect("decode i32 all-visible");
            black_box(decoded_rows);
            black_box(batch.num_rows());
        });
    });

    group.throughput(Throughput::Elements(f_i32_sparse.block.num_offsets as u64));
    group.bench_function("i32_every_other_visible", |b| {
        b.iter(|| {
            let (batch, decoded_rows) = decode_heap_page_block(
                black_box(&f_i32_sparse.block),
                &f_i32_sparse.schema,
                &f_i32_sparse.attrs,
                f_i32_sparse.proj_indices.as_deref(),
            )
            .expect("decode i32 sparse");
            black_box(decoded_rows);
            black_box(batch.num_rows());
        });
    });

    group.throughput(Throughput::Elements(f_i32_text.block.num_offsets as u64));
    group.bench_function("i32_text32_all_visible", |b| {
        b.iter(|| {
            let (batch, decoded_rows) = decode_heap_page_block(
                black_box(&f_i32_text.block),
                &f_i32_text.schema,
                &f_i32_text.attrs,
                f_i32_text.proj_indices.as_deref(),
            )
            .expect("decode i32+text");
            black_box(decoded_rows);
            black_box(batch.num_rows());
        });
    });

    group.finish();
}

criterion_group!(benches, bench_heap_decode);
criterion_main!(benches);
