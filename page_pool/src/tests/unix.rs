use super::cfg;
use crate::{PageDescriptor, PagePool};
use std::env;
use std::fs::{remove_file, File, OpenOptions};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::ptr::NonNull;
use std::time::{SystemTime, UNIX_EPOCH};

const SENTINEL: [u8; 8] = 0x1122_3344_5566_7788u64.to_le_bytes();
const CHILD_TEST_NAME: &str = "tests::unix::multiprocess_child_entry";

struct SharedFileRegion {
    path: PathBuf,
    base: NonNull<u8>,
    len: usize,
    remove_on_drop: bool,
}

impl SharedFileRegion {
    fn create(len: usize) -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock drift")
            .as_nanos();
        let path =
            env::temp_dir().join(format!("page_pool_test_{}_{}", std::process::id(), unique));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .expect("create shared file");
        file.set_len(len as u64).expect("resize shared file");
        let base = unsafe { map_shared(&file, len) };
        Self {
            path,
            base,
            len,
            remove_on_drop: true,
        }
    }

    fn open(path: &Path, len: usize) -> Self {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .expect("open shared file");
        let base = unsafe { map_shared(&file, len) };
        Self {
            path: path.to_path_buf(),
            base,
            len,
            remove_on_drop: false,
        }
    }
}

impl Drop for SharedFileRegion {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.base.as_ptr().cast(), self.len);
        }
        if self.remove_on_drop {
            let _ = remove_file(&self.path);
        }
    }
}

unsafe fn map_shared(file: &File, len: usize) -> NonNull<u8> {
    let ptr = libc::mmap(
        std::ptr::null_mut(),
        len,
        libc::PROT_READ | libc::PROT_WRITE,
        libc::MAP_SHARED,
        file.as_raw_fd(),
        0,
    );
    assert_ne!(ptr, libc::MAP_FAILED, "mmap failed");
    NonNull::new(ptr.cast()).expect("mmap returned null")
}

#[test]
fn multiprocess_child_entry() {
    if env::var_os("PAGE_POOL_CHILD").is_none() {
        return;
    }

    let path = PathBuf::from(env::var("PAGE_POOL_FILE").expect("file env"));
    let len: usize = env::var("PAGE_POOL_LEN")
        .expect("len env")
        .parse()
        .expect("len parse");
    let page_id: u32 = env::var("PAGE_POOL_PAGE_ID")
        .expect("page id env")
        .parse()
        .expect("page id parse");
    let pool_id: u64 = env::var("PAGE_POOL_POOL_ID")
        .expect("pool id env")
        .parse()
        .expect("pool id parse");
    let generation: u64 = env::var("PAGE_POOL_GENERATION")
        .expect("generation env")
        .parse()
        .expect("generation parse");

    let region = SharedFileRegion::open(&path, len);
    let pool = unsafe { PagePool::attach(region.base, region.len) }.expect("attach");
    let desc = PageDescriptor {
        pool_id,
        page_id,
        generation,
    };
    let bytes = unsafe { pool.page_bytes_mut(desc) }.expect("page bytes mut");
    bytes[..SENTINEL.len()].copy_from_slice(&SENTINEL);
    pool.release(desc).expect("release from child");
}

#[test]
fn multiprocess_handoff_roundtrip() {
    let config = cfg(64, 1);
    let layout = PagePool::layout(config).expect("layout");
    let region = SharedFileRegion::create(layout.size);
    let pool = unsafe { PagePool::init_in_place(region.base, region.len, config) }.expect("init");

    let desc = pool
        .try_acquire()
        .expect("lease")
        .into_descriptor()
        .expect("detach");
    let status = Command::new(env::current_exe().expect("current exe"))
        .arg("--exact")
        .arg(CHILD_TEST_NAME)
        .arg("--nocapture")
        .env("PAGE_POOL_CHILD", "1")
        .env("PAGE_POOL_FILE", &region.path)
        .env("PAGE_POOL_LEN", region.len.to_string())
        .env("PAGE_POOL_POOL_ID", desc.pool_id.to_string())
        .env("PAGE_POOL_PAGE_ID", desc.page_id.to_string())
        .env("PAGE_POOL_GENERATION", desc.generation.to_string())
        .status()
        .expect("spawn child test");
    assert!(status.success(), "child test failed: {status}");

    let lease = pool.try_acquire().expect("reacquire");
    assert_eq!(lease.page_id(), 0);
    assert_eq!(&lease.bytes()[..SENTINEL.len()], &SENTINEL);
}
