use pgrx::prelude::*;

::pgrx::pg_module_magic!();

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use anyhow::Result as AnyResult;
    use datafusion_common::ScalarValue;
    use pgrx::prelude::*;
    use std::{os::raw::c_void, ptr};
    use storage::heap::{decode_tuple_project, HeapPage, PgAttrMeta};

    // ---------- Helpers to reduce test boilerplate ----------
    // NOTE: Avoid direct TupleDesc manipulation in tests for portability across PG versions.

    /// Open a relation by qualified name (eg. "public.my_table"). Caller must close.
    unsafe fn relation_open_by_name(qualified: &str) -> pg_sys::Relation {
        let sql = format!("SELECT '{}'::regclass::oid", qualified);
        let relid: pg_sys::Oid = Spi::get_one(&sql).unwrap().unwrap();
        pg_sys::relation_open(relid, pg_sys::AccessShareLock as pg_sys::LOCKMODE)
    }

    /// Decode all rows for the given projection from a qualified table using provided attrs.
    fn decode_all_rows_with_attrs(
        qualified: &str,
        attrs: &[PgAttrMeta],
        projection: &[usize],
    ) -> AnyResult<Vec<Vec<ScalarValue>>> {
        unsafe {
            let rel = relation_open_by_name(qualified);
            let nblocks =
                pg_sys::RelationGetNumberOfBlocksInFork(rel, pg_sys::ForkNumber::MAIN_FORKNUM);
            let mut rows = Vec::new();
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
                let base = page as *const u8;
                let slice = std::slice::from_raw_parts(base, pg_sys::BLCKSZ as usize);
                let hp = HeapPage::from_slice(slice)?;
                for tup_bytes in hp.tuples(None, ptr::null_mut::<c_void>()) {
                    let it = decode_tuple_project(page_hdr, tup_bytes, attrs, projection)?;
                    let vals: Result<Vec<_>, _> = it.collect();
                    rows.push(vals?);
                }
                pg_sys::UnlockReleaseBuffer(buf);
            }
            pg_sys::relation_close(rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
            Ok(rows)
        }
    }

    /// Return true if decoding the given projection produces an error for at least one tuple.
    fn decode_expect_error_with_attrs(
        qualified: &str,
        attrs: &[PgAttrMeta],
        projection: &[usize],
    ) -> bool {
        unsafe {
            let rel = relation_open_by_name(qualified);
            let nblocks =
                pg_sys::RelationGetNumberOfBlocksInFork(rel, pg_sys::ForkNumber::MAIN_FORKNUM);
            let mut saw_err = false;
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
                let base = page as *const u8;
                let slice = std::slice::from_raw_parts(base, pg_sys::BLCKSZ as usize);
                if let Ok(hp) = HeapPage::from_slice(slice) {
                    'tuples: for tup_bytes in hp.tuples(None, ptr::null_mut::<c_void>()) {
                        match decode_tuple_project(page_hdr, tup_bytes, attrs, projection) {
                            Ok(mut it) => {
                                if let Some(Err(_)) = it.next() {
                                    saw_err = true;
                                    break 'tuples;
                                }
                            }
                            Err(_) => {
                                saw_err = true;
                                break 'tuples;
                            }
                        }
                    }
                }
                pg_sys::UnlockReleaseBuffer(buf);
                if saw_err {
                    break;
                }
            }
            pg_sys::relation_close(rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
            saw_err
        }
    }

    /// Build PgAttrMeta vector for a relation by querying pg_attribute.
    /// Skips dropped columns; orders by attnum. Intended for tests.
    fn attrs_from_pg_attribute(qualified: &str) -> Vec<PgAttrMeta> {
        // Determine max attnum to iterate through; cap to a reasonable bound to avoid long loops
        let max_attnum: i32 = Spi::get_one::<i32>(&format!(
            "SELECT coalesce(max(attnum), 0) FROM pg_attribute WHERE attrelid = '{}'::regclass AND attnum > 0",
            qualified
        ))
        .unwrap()
        .unwrap_or(0);

        let mut out = Vec::new();
        for attnum in 1..=max_attnum {
            // Skip dropped
            let is_dropped: Option<bool> = Spi::get_one(&format!(
                "SELECT attisdropped FROM pg_attribute WHERE attrelid = '{}'::regclass AND attnum = {}",
                qualified, attnum
            ))
            .unwrap();
            if matches!(is_dropped, Some(true)) {
                continue;
            }

            // atttypid as Oid (i32), attlen (i16), attalign as text
            let atttypid_i32: Option<i32> = Spi::get_one(&format!(
                "SELECT atttypid::oid::int4 FROM pg_attribute WHERE attrelid = '{}'::regclass AND attnum = {} AND NOT attisdropped",
                qualified, attnum
            ))
            .unwrap();
            let attlen: Option<i16> = Spi::get_one(&format!(
                "SELECT attlen::int2 FROM pg_attribute WHERE attrelid = '{}'::regclass AND attnum = {} AND NOT attisdropped",
                qualified, attnum
            ))
            .unwrap();
            let attalign_txt: Option<String> = Spi::get_one(&format!(
                "SELECT attalign::text FROM pg_attribute WHERE attrelid = '{}'::regclass AND attnum = {} AND NOT attisdropped",
                qualified, attnum
            ))
            .unwrap();

            if let (Some(oid_i32), Some(len), Some(align_s)) = (atttypid_i32, attlen, attalign_txt)
            {
                let atttypid = pg_sys::Oid::from(oid_i32 as u32);
                let attalign = align_s.as_bytes().get(0).copied().unwrap_or(b'i');
                out.push(PgAttrMeta {
                    atttypid,
                    attlen: len,
                    attalign,
                });
            }
        }
        out
    }

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

        let attrs = attrs_from_pg_attribute("public.heap_decode_t");
        let projection: Vec<usize> = vec![0, 2, 6, 7, 8, 9, 10];
        let rows = decode_all_rows_with_attrs("public.heap_decode_t", &attrs, &projection).unwrap();
        assert!(rows.len() >= 2);
        assert_eq!(rows[0][0], ScalarValue::Boolean(Some(true)));
        assert_eq!(rows[0][1], ScalarValue::Int32(Some(2)));
        assert_eq!(rows[0][2], ScalarValue::Utf8(Some("hi".to_string())));
        assert_eq!(rows[0][3], ScalarValue::Date32(Some(10_958)));
        assert_eq!(rows[0][4], ScalarValue::Time64Microsecond(Some(1_000_000)));
        assert_eq!(
            rows[0][5],
            ScalarValue::TimestampMicrosecond(Some(946_684_801_000_000), None)
        );
        assert_eq!(
            rows[0][6],
            ScalarValue::IntervalMonthDayNano(Some(
                datafusion_common::arrow::array::types::IntervalMonthDayNano {
                    months: 1,
                    days: 2,
                    nanoseconds: 3_000_000_000
                }
            ))
        );
        assert_eq!(rows[1][0], ScalarValue::Boolean(None));
        assert_eq!(rows[1][1], ScalarValue::Int32(Some(42)));
        assert_eq!(rows[1][2], ScalarValue::Utf8(None));
        assert_eq!(rows[1][3], ScalarValue::Date32(Some(10_957)));
        assert_eq!(rows[1][4], ScalarValue::Time64Microsecond(Some(0)));
        assert_eq!(
            rows[1][5],
            ScalarValue::TimestampMicrosecond(Some(946_684_800_000_000), None)
        );
        assert_eq!(
            rows[1][6],
            ScalarValue::IntervalMonthDayNano(Some(
                datafusion_common::arrow::array::types::IntervalMonthDayNano {
                    months: 0,
                    days: 0,
                    nanoseconds: 0
                }
            ))
        );
    }

    #[pg_test]
    fn heap_decode_projection_errors_and_skip() {
        Spi::run("DROP TABLE IF EXISTS public.heap_decode_toast_t").unwrap();
        Spi::run(
            "CREATE TABLE public.heap_decode_toast_t (id int4, t_comp text, t_ext text, post int4)",
        )
        .unwrap();
        Spi::run("INSERT INTO public.heap_decode_toast_t VALUES (1, repeat('a', 10000), repeat('b', 200000), 42)").unwrap();

        let toast_attrs = attrs_from_pg_attribute("public.heap_decode_toast_t");
        // Expect errors when projecting toasted/compressed columns
        assert!(decode_expect_error_with_attrs(
            "public.heap_decode_toast_t",
            &toast_attrs,
            &[1]
        ));
        assert!(decode_expect_error_with_attrs(
            "public.heap_decode_toast_t",
            &toast_attrs,
            &[2]
        ));
        // Skipping toasted columns should allow decoding other attributes
        let rows = decode_all_rows_with_attrs("public.heap_decode_toast_t", &toast_attrs, &[0, 3])
            .unwrap();
        assert_eq!(rows[0][0], ScalarValue::Int32(Some(1)));
        assert_eq!(rows[0][1], ScalarValue::Int32(Some(42)));
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
            let attrs = attrs_from_pg_attribute("public.heap_decode_date_t");
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
