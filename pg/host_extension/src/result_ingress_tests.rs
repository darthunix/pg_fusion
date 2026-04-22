use pgrx::pg_sys;
use pgrx::prelude::*;

use crate::result_ingress::debug_repro;

const RESULT_INGRESS_FIXTURE: &str = "pg_temp.pgf_result_ingress_fixture";

struct OpenRelation {
    rel: pg_sys::Relation,
}

impl OpenRelation {
    fn open(qualified: &str) -> Self {
        let rel = unsafe { relation_open_by_name(qualified) };
        Self { rel }
    }

    fn tuple_desc(&self) -> pg_sys::TupleDesc {
        unsafe { (*self.rel).rd_att }
    }
}

impl Drop for OpenRelation {
    fn drop(&mut self) {
        unsafe {
            pg_sys::relation_close(self.rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
        }
    }
}

pub(crate) fn result_ingress_roundtrip_smoke() {
    Spi::run(&format!("DROP TABLE IF EXISTS {RESULT_INGRESS_FIXTURE}"))
        .expect("drop temp result_ingress fixture must succeed");
    Spi::run(&format!(
        "CREATE TEMP TABLE {RESULT_INGRESS_FIXTURE} (id bigint NOT NULL, payload text NOT NULL)"
    ))
    .expect("create temp result_ingress fixture must succeed");

    let relation = OpenRelation::open(RESULT_INGRESS_FIXTURE);
    unsafe {
        debug_repro::single_page_result_ingress_roundtrip(relation.tuple_desc())
            .unwrap_or_else(|err| panic!("result_ingress roundtrip repro failed: {err}"));
    }
}

unsafe fn relation_open_by_name(qualified: &str) -> pg_sys::Relation {
    let sql = format!("SELECT '{qualified}'::regclass::oid");
    let relid: pg_sys::Oid = Spi::get_one(&sql)
        .expect("resolve regclass query must succeed")
        .expect("fixture relation regclass query must return one row");
    pg_sys::relation_open(relid, pg_sys::AccessShareLock as pg_sys::LOCKMODE)
}
