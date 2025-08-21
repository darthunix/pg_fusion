use pgrx::prelude::*;

::pgrx::pg_module_magic!();

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use datafusion_common::ScalarValue;
    use pgrx::prelude::*;
    use std::{os::raw::c_void, ptr};
    use storage::heap::{decode_tuple_project, HeapPage, PgAttrMeta};

    #[pg_test]
    fn test_heap_block_iteration() {
        Spi::run("DROP TABLE IF EXISTS public.heap_iter_t").unwrap();
        Spi::run("CREATE TABLE public.heap_iter_t (id int, payload text)").unwrap();
        let nrows = 300i32;
        Spi::run(&format!(
            "INSERT INTO public.heap_iter_t SELECT g, repeat('x', 20) FROM generate_series(1, {}) g",
            nrows
        ))
        .unwrap();
        let expected: i64 = Spi::get_one("SELECT count(*) FROM public.heap_iter_t")
            .unwrap()
            .unwrap();

        unsafe {
            let relid: pg_sys::Oid = Spi::get_one("SELECT 'public.heap_iter_t'::regclass::oid")
                .unwrap()
                .unwrap();
            let rel = pg_sys::relation_open(relid, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
            let nblocks =
                pg_sys::RelationGetNumberOfBlocksInFork(rel, pg_sys::ForkNumber::MAIN_FORKNUM);
            let mut total = 0_i64;
            for blkno in 0..nblocks {
                let buf = pg_sys::ReadBufferExtended(
                    rel,
                    pg_sys::ForkNumber::MAIN_FORKNUM,
                    blkno,
                    pg_sys::ReadBufferMode::RBM_NORMAL,
                    ptr::null_mut(),
                );
                pg_sys::LockBuffer(buf, pg_sys::BUFFER_LOCK_SHARE as i32);

                let page = pg_sys::BufferGetPage(buf);
                let ptr = page as *const u8;
                let slice = std::slice::from_raw_parts(ptr, pg_sys::BLCKSZ as usize);
                let hp = HeapPage::from_slice(slice).unwrap();

                let c = hp.tuples(None, ptr::null_mut::<c_void>()).count();
                total += c as i64;
                pg_sys::UnlockReleaseBuffer(buf);
            }
            pg_sys::relation_close(rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
            assert_eq!(total, expected);
        }
    }

    #[pg_test]
    fn heap_decode_projection_iter() {
        Spi::run("DROP TABLE IF EXISTS public.heap_decode_t").unwrap();
        Spi::run(
            "CREATE TABLE public.heap_decode_t (
            b boolean,
            s int2,
            i int4,
            l int8,
            r real,
            d double precision,
            t text,
            dt date,
            tm time,
            ts timestamp,
            iv interval
        )",
        )
        .unwrap();

        Spi::run("INSERT INTO public.heap_decode_t VALUES (
            true, 1, 2, 3, 1.5::real, 2.5::double precision,
            'hi', DATE '2000-01-02', TIME '00:00:01', TIMESTAMP '2000-01-01 00:00:01', INTERVAL '1 month 2 days 3 seconds'
        )").unwrap();
        Spi::run("INSERT INTO public.heap_decode_t VALUES (
            NULL, NULL, 42, 10000000000, 7.0::real, 8.0::double precision,
            NULL, DATE '2000-01-01', TIME '00:00:00', TIMESTAMP '2000-01-01 00:00:00', INTERVAL '0 months 0 days 0 seconds'
        )").unwrap();

        unsafe {
            let relid: pg_sys::Oid = Spi::get_one("SELECT 'public.heap_decode_t'::regclass::oid")
                .unwrap()
                .unwrap();
            let rel = pg_sys::relation_open(relid, pg_sys::AccessShareLock as pg_sys::LOCKMODE);

            let attrs: Vec<PgAttrMeta> = vec![
                PgAttrMeta {
                    atttypid: pg_sys::BOOLOID,
                    attlen: 1,
                    attalign: b'c',
                },
                PgAttrMeta {
                    atttypid: pg_sys::INT2OID,
                    attlen: 2,
                    attalign: b's',
                },
                PgAttrMeta {
                    atttypid: pg_sys::INT4OID,
                    attlen: 4,
                    attalign: b'i',
                },
                PgAttrMeta {
                    atttypid: pg_sys::INT8OID,
                    attlen: 8,
                    attalign: b'd',
                },
                PgAttrMeta {
                    atttypid: pg_sys::FLOAT4OID,
                    attlen: 4,
                    attalign: b'i',
                },
                PgAttrMeta {
                    atttypid: pg_sys::FLOAT8OID,
                    attlen: 8,
                    attalign: b'd',
                },
                PgAttrMeta {
                    atttypid: pg_sys::TEXTOID,
                    attlen: -1,
                    attalign: b'i',
                },
                PgAttrMeta {
                    atttypid: pg_sys::DATEOID,
                    attlen: 4,
                    attalign: b'i',
                },
                PgAttrMeta {
                    atttypid: pg_sys::TIMEOID,
                    attlen: 8,
                    attalign: b'd',
                },
                PgAttrMeta {
                    atttypid: pg_sys::TIMESTAMPOID,
                    attlen: 8,
                    attalign: b'd',
                },
                PgAttrMeta {
                    atttypid: pg_sys::INTERVALOID,
                    attlen: 16,
                    attalign: b'd',
                },
            ];

            let projection: Vec<usize> = vec![0, 2, 6, 7, 8, 9, 10];

            let nblocks =
                pg_sys::RelationGetNumberOfBlocksInFork(rel, pg_sys::ForkNumber::MAIN_FORKNUM);
            let mut seen_rows = 0usize;
            for blkno in 0..nblocks {
                let buf = pg_sys::ReadBufferExtended(
                    rel,
                    pg_sys::ForkNumber::MAIN_FORKNUM,
                    blkno,
                    pg_sys::ReadBufferMode::RBM_NORMAL,
                    ptr::null_mut(),
                );
                pg_sys::LockBuffer(buf, pg_sys::BUFFER_LOCK_SHARE as i32);

                let page = pg_sys::BufferGetPage(buf);
                let page_hdr = page as *const pg_sys::PageHeaderData;
                let ptr = page as *const u8;
                let slice = std::slice::from_raw_parts(ptr, pg_sys::BLCKSZ as usize);
                let hp = HeapPage::from_slice(slice).unwrap();

                for tup_bytes in hp.tuples(None, ptr::null_mut::<c_void>()) {
                    let iter =
                        decode_tuple_project(page_hdr, tup_bytes, &attrs, &projection).unwrap();
                    let vals: Vec<_> = iter.map(|r| r.unwrap()).collect();

                    if seen_rows == 0 {
                        assert_eq!(vals[0], ScalarValue::Boolean(Some(true)));
                        assert_eq!(vals[1], ScalarValue::Int32(Some(2)));
                        assert_eq!(vals[2], ScalarValue::Utf8(Some("hi".to_string())));
                        assert_eq!(vals[3], ScalarValue::Date32(Some(10_958)));
                        assert_eq!(vals[4], ScalarValue::Time64Microsecond(Some(1_000_000)));
                        assert_eq!(
                            vals[5],
                            ScalarValue::TimestampMicrosecond(Some(946_684_801_000_000), None)
                        );
                        assert_eq!(
                            vals[6],
                            ScalarValue::IntervalMonthDayNano(Some(
                                datafusion_common::arrow::array::types::IntervalMonthDayNano {
                                    months: 1,
                                    days: 2,
                                    nanoseconds: 3_000_000_000
                                }
                            ))
                        );
                    } else if seen_rows == 1 {
                        assert_eq!(vals[0], ScalarValue::Boolean(None));
                        assert_eq!(vals[1], ScalarValue::Int32(Some(42)));
                        assert_eq!(vals[2], ScalarValue::Utf8(None));
                        assert_eq!(vals[3], ScalarValue::Date32(Some(10_957)));
                        assert_eq!(vals[4], ScalarValue::Time64Microsecond(Some(0)));
                        assert_eq!(
                            vals[5],
                            ScalarValue::TimestampMicrosecond(Some(946_684_800_000_000), None)
                        );
                        assert_eq!(
                            vals[6],
                            ScalarValue::IntervalMonthDayNano(Some(
                                datafusion_common::arrow::array::types::IntervalMonthDayNano {
                                    months: 0,
                                    days: 0,
                                    nanoseconds: 0
                                }
                            ))
                        );
                    }
                    seen_rows += 1;
                }

                pg_sys::UnlockReleaseBuffer(buf);
            }

            pg_sys::relation_close(rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
            assert!(seen_rows >= 2);
        }
    }

    #[pg_test]
    fn heap_decode_projection_errors_and_skip() {
        Spi::run("DROP TABLE IF EXISTS public.heap_decode_toast_t").unwrap();
        Spi::run(
            "CREATE TABLE public.heap_decode_toast_t (
            id int4,
            t_comp text,
            t_ext  text,
            post   int4
        )",
        )
        .unwrap();

        // Highly compressible and a very large externalized value
        Spi::run(
            "INSERT INTO public.heap_decode_toast_t VALUES (
            1,
            repeat('a', 10000),
            repeat('b', 200000),
            42
        )",
        )
        .unwrap();

        unsafe {
            let relid: pg_sys::Oid =
                Spi::get_one("SELECT 'public.heap_decode_toast_t'::regclass::oid")
                    .unwrap()
                    .unwrap();
            let rel = pg_sys::relation_open(relid, pg_sys::AccessShareLock as pg_sys::LOCKMODE);

            let attrs: Vec<PgAttrMeta> = vec![
                PgAttrMeta {
                    atttypid: pg_sys::INT4OID,
                    attlen: 4,
                    attalign: b'i',
                },
                PgAttrMeta {
                    atttypid: pg_sys::TEXTOID,
                    attlen: -1,
                    attalign: b'i',
                },
                PgAttrMeta {
                    atttypid: pg_sys::TEXTOID,
                    attlen: -1,
                    attalign: b'i',
                },
                PgAttrMeta {
                    atttypid: pg_sys::INT4OID,
                    attlen: 4,
                    attalign: b'i',
                },
            ];

            let nblocks =
                pg_sys::RelationGetNumberOfBlocksInFork(rel, pg_sys::ForkNumber::MAIN_FORKNUM);
            for blkno in 0..nblocks {
                let buf = pg_sys::ReadBufferExtended(
                    rel,
                    pg_sys::ForkNumber::MAIN_FORKNUM,
                    blkno,
                    pg_sys::ReadBufferMode::RBM_NORMAL,
                    ptr::null_mut(),
                );
                pg_sys::LockBuffer(buf, pg_sys::BUFFER_LOCK_SHARE as i32);

                let page = pg_sys::BufferGetPage(buf);
                let page_hdr = page as *const pg_sys::PageHeaderData;
                let ptr = page as *const u8;
                let slice = std::slice::from_raw_parts(ptr, pg_sys::BLCKSZ as usize);
                let hp = HeapPage::from_slice(slice).unwrap();

                for tup_bytes in hp.tuples(None, ptr::null_mut::<c_void>()) {
                    // 1) Project t_comp only — expect error (compressed inline)
                    let proj_comp = [1usize];
                    let mut it =
                        decode_tuple_project(page_hdr, tup_bytes, &attrs, &proj_comp).unwrap();
                    match it.next() {
                        Some(Err(_)) => {}
                        other => panic!("expected error for compressed varlena, got {:?}", other),
                    }

                    // 2) Project t_ext only — expect error (external toast)
                    let proj_ext = [2usize];
                    let mut it =
                        decode_tuple_project(page_hdr, tup_bytes, &attrs, &proj_ext).unwrap();
                    match it.next() {
                        Some(Err(_)) => {}
                        other => panic!("expected error for external varlena, got {:?}", other),
                    }

                    // 3) Skip both varlena columns; expect id and post without errors
                    let proj_ok = [0usize, 3usize];
                    let mut it =
                        decode_tuple_project(page_hdr, tup_bytes, &attrs, &proj_ok).unwrap();
                    match (it.next(), it.next(), it.next()) {
                        (Some(Ok(v1)), Some(Ok(v2)), None) => {
                            assert_eq!(v1, ScalarValue::Int32(Some(1)));
                            assert_eq!(v2, ScalarValue::Int32(Some(42)));
                        }
                        other => panic!("unexpected decode sequence: {:?}", other),
                    }
                }

                pg_sys::UnlockReleaseBuffer(buf);
            }

            pg_sys::relation_close(rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
        }
    }

    #[pg_test]
    fn heap_decode_date_extremes() {
        Spi::run("DROP TABLE IF EXISTS public.heap_decode_date_t").unwrap();
        Spi::run(
            "CREATE TABLE public.heap_decode_date_t (
                id int4,
                dt date
            )",
        )
        .unwrap();

        // Insert a set of extreme and representative dates
        Spi::run(
            "INSERT INTO public.heap_decode_date_t VALUES
                (1, DATE '1970-01-01'),
                (2, DATE '2000-01-01'),
                (3, DATE '4713-01-01 BC'),
                (4, DATE '5874897-12-31'),
                (5, '-infinity'::date),
                (6, 'infinity'::date),
                (7, NULL)
            ",
        )
        .unwrap();

        unsafe {
            // Build expected map id -> expected days since 1970-01-01 (use SPI per id)
            let mut expected: std::collections::HashMap<i32, i32> =
                std::collections::HashMap::new();
            for id in 1..=6 {
                let q = format!(
                    "SELECT CASE WHEN dt = '-infinity'::date THEN -2147483648 WHEN dt = 'infinity'::date THEN 2147483647 ELSE (dt - DATE '1970-01-01')::int END FROM public.heap_decode_date_t WHERE id = {}",
                    id
                );
                let days: i32 = Spi::get_one(&q).unwrap().unwrap();
                expected.insert(id, days);
            }

            // Prepare attribute metadata and projection [id, dt]
            let attrs: Vec<PgAttrMeta> = vec![
                PgAttrMeta {
                    atttypid: pg_sys::INT4OID,
                    attlen: 4,
                    attalign: b'i',
                },
                PgAttrMeta {
                    atttypid: pg_sys::DATEOID,
                    attlen: 4,
                    attalign: b'i',
                },
            ];
            let proj: [usize; 2] = [0, 1];

            let relid: pg_sys::Oid =
                Spi::get_one("SELECT 'public.heap_decode_date_t'::regclass::oid")
                    .unwrap()
                    .unwrap();
            let rel = pg_sys::relation_open(relid, pg_sys::AccessShareLock as pg_sys::LOCKMODE);

            let nblocks =
                pg_sys::RelationGetNumberOfBlocksInFork(rel, pg_sys::ForkNumber::MAIN_FORKNUM);
            let mut verified = 0usize;
            for blkno in 0..nblocks {
                let buf = pg_sys::ReadBufferExtended(
                    rel,
                    pg_sys::ForkNumber::MAIN_FORKNUM,
                    blkno,
                    pg_sys::ReadBufferMode::RBM_NORMAL,
                    ptr::null_mut(),
                );
                pg_sys::LockBuffer(buf, pg_sys::BUFFER_LOCK_SHARE as i32);

                let page = pg_sys::BufferGetPage(buf);
                let page_hdr = page as *const pg_sys::PageHeaderData;
                let ptr = page as *const u8;
                let slice = std::slice::from_raw_parts(ptr, pg_sys::BLCKSZ as usize);
                let hp = HeapPage::from_slice(slice).unwrap();

                for tup_bytes in hp.tuples(None, ptr::null_mut::<c_void>()) {
                    let iter =
                        storage::heap::decode_tuple_project(page_hdr, tup_bytes, &attrs, &proj)
                            .unwrap();
                    let vals: Vec<_> = iter.map(|r| r.unwrap()).collect();
                    // Extract id and days
                    if let (ScalarValue::Int32(Some(id)), date_val) = (&vals[0], &vals[1]) {
                        match date_val {
                            ScalarValue::Date32(Some(days)) => {
                                if let Some(exp) = expected.get(id) {
                                    assert_eq!(*days, *exp, "mismatch for id {}", id);
                                }
                            }
                            ScalarValue::Date32(None) => {
                                // id 7 is NULL
                                assert_eq!(*id, 7);
                            }
                            other => panic!("unexpected date scalar: {:?}", other),
                        }
                        verified += 1;
                    }
                }

                pg_sys::UnlockReleaseBuffer(buf);
            }

            pg_sys::relation_close(rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
            assert!(verified >= 7);
        }
    }
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
