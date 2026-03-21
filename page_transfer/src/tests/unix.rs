use super::{cfg, encode_frame, ReceiveEvent, TEST_FLAGS, TEST_KIND};
use crate::{FrameDecoder, PageRx, PageTx};
use std::env;
use std::fs::{remove_file, File, OpenOptions};
use std::io::Write;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::ptr::NonNull;
use std::time::{SystemTime, UNIX_EPOCH};

const SENTINEL: &[u8] = b"cross-process-page";
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
        let path = env::temp_dir().join(format!(
            "page_transfer_test_{}_{}",
            std::process::id(),
            unique
        ));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .expect("create file");
        file.set_len(len as u64).expect("resize file");
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
            .expect("open file");
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

fn to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = std::fmt::Write::write_fmt(&mut out, format_args!("{byte:02x}"));
    }
    out
}

fn from_hex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let hi = (pair[0] as char).to_digit(16).expect("hex hi") as u8;
            let lo = (pair[1] as char).to_digit(16).expect("hex lo") as u8;
            (hi << 4) | lo
        })
        .collect()
}

#[test]
fn multiprocess_child_entry() {
    if env::var_os("PAGE_TRANSFER_CHILD").is_none() {
        return;
    }

    let path = PathBuf::from(env::var("PAGE_TRANSFER_FILE").expect("file env"));
    let len: usize = env::var("PAGE_TRANSFER_LEN")
        .expect("len env")
        .parse()
        .expect("len parse");
    let encoded = from_hex(&env::var("PAGE_TRANSFER_FRAME").expect("frame env"));

    let region = SharedFileRegion::open(&path, len);
    let pool = unsafe { page_pool::PagePool::attach(region.base, region.len) }.expect("attach");
    let rx = PageRx::new(pool);
    let mut decoder = FrameDecoder::new();
    let frame = {
        let mut frames = decoder.push(&encoded);
        let frame = frames.next().expect("frame").expect("decode");
        assert!(frames.next().is_none());
        frame
    };

    match rx.accept(frame).expect("accept frame") {
        ReceiveEvent::Page(page) => {
            assert_eq!(page.kind(), TEST_KIND);
            assert_eq!(page.flags(), TEST_FLAGS);
            assert_eq!(page.payload(), SENTINEL);
            page.release().expect("release");
        }
        ReceiveEvent::Closed => panic!("expected page"),
    };
}

#[test]
fn multiprocess_handoff_roundtrip() {
    let config = cfg(128, 1);
    let layout = page_pool::PagePool::layout(config).expect("layout");
    let region = SharedFileRegion::create(layout.size);
    let pool = unsafe { page_pool::PagePool::init_in_place(region.base, region.len, config) }
        .expect("init");
    let tx = PageTx::new(pool);

    let mut writer = tx.begin(TEST_KIND, TEST_FLAGS).expect("begin");
    writer.write_all(SENTINEL).expect("write sentinel");
    let outbound = writer.finish().expect("finish");
    let frame = encode_frame(outbound.frame()).expect("encode frame");
    outbound.mark_sent();

    let status = Command::new(env::current_exe().expect("current exe"))
        .arg("--exact")
        .arg(CHILD_TEST_NAME)
        .arg("--nocapture")
        .env("PAGE_TRANSFER_CHILD", "1")
        .env("PAGE_TRANSFER_FILE", &region.path)
        .env("PAGE_TRANSFER_LEN", region.len.to_string())
        .env("PAGE_TRANSFER_FRAME", to_hex(&frame))
        .status()
        .expect("spawn child");
    assert!(status.success(), "child test failed: {status}");

    let tx = PageTx::new(pool);
    let writer = tx.begin(TEST_KIND, TEST_FLAGS).expect("reacquire writer");
    assert_eq!(writer.remaining(), 128 - crate::page::PAGE_HEADER_LEN);
    assert_eq!(pool.snapshot().free_pages, 0);
    drop(writer);
    assert_eq!(pool.snapshot().free_pages, 1);
}
