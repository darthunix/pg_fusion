use crate::ipc::{aligned_offsets, Bus, Slot, SlotNumber, SlotStream, DATA_SIZE};
use crate::worker::worker_id;
use pgrx::pg_sys::ProcSendSignal;
use pgrx::prelude::*;
use std::io::Write;
use std::ops::Range;
use std::slice::from_raw_parts;

#[repr(C)]
#[derive(Clone, Default, PartialEq)]
pub(crate) enum Direction {
    #[default]
    ToWorker = 0,
    FromWorker = 1,
}

#[repr(C)]
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) enum Packet {
    Error = 1,
    Parse = 2,
    #[default]
    None = 0,
}

#[repr(C)]
#[derive(Clone, Default, Debug, PartialEq)]
pub(crate) enum Flag {
    More = 0,
    #[default]
    Last = 1,
}

#[repr(C)]
#[derive(Default)]
pub(crate) struct Header {
    pub(crate) direction: Direction,
    pub(crate) packet: Packet,
    pub(crate) length: u16,
    pub(crate) flag: Flag,
}

impl Header {
    const fn estimate_size() -> usize {
        std::mem::size_of::<Self>()
    }

    const fn payload_max_size() -> usize {
        DATA_SIZE - Self::estimate_size()
    }

    fn range1() -> Range<usize> {
        aligned_offsets::<Direction>(0)
    }

    fn range2() -> Range<usize> {
        let Range { start: _, end } = Self::range1();
        aligned_offsets::<Packet>(end)
    }

    fn range3() -> Range<usize> {
        let Range { start: _, end } = Self::range2();
        aligned_offsets::<u16>(end)
    }

    fn range4() -> Range<usize> {
        let Range { start: _, end } = Self::range3();
        aligned_offsets::<Flag>(end)
    }

    fn as_bytes(&self) -> &[u8] {
        unsafe { from_raw_parts(self as *const Self as *const u8, Self::estimate_size()) }
    }

    pub(crate) fn from_bytes(buffer: &[u8]) -> Self {
        assert_eq!(Self::estimate_size(), buffer.len());
        unsafe {
            let direction = buffer[Self::range1()].as_ptr() as *const Direction;
            let packet = buffer[Self::range2()].as_ptr() as *const Packet;
            let length = buffer[Self::range3()].as_ptr() as *const u16;
            let flag = buffer[Self::range4()].as_ptr() as *const Flag;
            let header = Header {
                direction: (&*direction).clone(),
                packet: (&*packet).clone(),
                length: *length,
                flag: (&*flag).clone(),
            };
            header
        }
    }
}

fn signal(slot_id: SlotNumber, direction: Direction) {
    match direction {
        Direction::ToWorker => {
            unsafe { ProcSendSignal(worker_id()) };
        }
        Direction::FromWorker => {
            let id = Bus::new().slot(slot_id).owner();
            unsafe { ProcSendSignal(id) };
        }
    }
}

#[allow(clippy::unused_io_amount)]
pub(crate) fn send(
    slot_id: SlotNumber,
    mut stream: SlotStream,
    direction: Direction,
    packet: Packet,
    flag: Flag,
    payload: &[u8],
) {
    // FIXME: split into multiple messages if the payload is too large.
    if payload.len() > Header::payload_max_size() {
        panic!("payload is too long");
    }
    let length = payload.len() as u16;
    let header = Header {
        direction: direction.clone(),
        packet,
        length,
        flag,
    };
    let header_bytes = header.as_bytes();
    let stream_ptr = &mut stream;
    stream_ptr.flush().expect("Failed to flush stream");
    stream_ptr
        .write(header_bytes)
        .expect("Failed to write header");
    stream_ptr.write(payload).expect("Failed to write payload");
    let _guard = Slot::from(stream);
    signal(slot_id, direction);
}

pub(crate) fn read_header(stream: &mut SlotStream) -> Header {
    let buffer = stream.look_ahead(Header::estimate_size()).unwrap();
    let header = Header::from_bytes(&buffer);
    stream.rewind(Header::estimate_size()).unwrap();
    header
}
