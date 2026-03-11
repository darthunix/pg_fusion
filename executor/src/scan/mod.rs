pub(crate) mod heap;

pub(crate) use heap::{
    count_heap_scans, for_each_heap_scan, HeapPageBlock, HeapScanProvider, HeapScanRegistry,
};
