use pgrx::prelude::*;

::pgrx::pg_module_magic!();

#[pg_extern]
fn hello_pg_test() -> &'static str {
    "Hello, pg_test"
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use std::{os::raw::c_void, ptr};

    use pgrx::prelude::*;
    use storage::heap::HeapPage;

    #[pg_test]
    fn test_hello_pg_test() {
        assert_eq!("Hello, pg_test", crate::hello_pg_test());
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
