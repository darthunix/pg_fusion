use crate::ipc::{aligned_offsets, Bus, Slot, SlotNumber, SlotStream, DATA_SIZE};
use crate::worker::worker_id;
use pgrx::pg_sys::ProcSendSignal;
use pgrx::prelude::*;
use std::io::{Read, Write};
use std::ops::Range;
use std::slice::{from_raw_parts, from_raw_parts_mut};

#[repr(C)]
#[derive(Clone, Default)]
pub(crate) enum Direction {
    #[default]
    ToWorker = 0,
    FromWorker = 1,
}

#[repr(C)]
#[derive(Clone, Default)]
pub(crate) enum Packet {
    Ack = 0,
    #[default]
    None = 1,
    Query = 2,
}

#[repr(C)]
#[derive(Clone, Default)]
pub(crate) enum Flag {
    More = 0,
    #[default]
    Last = 1,
}

#[repr(C)]
#[derive(Default)]
pub(crate) struct Header {
    direction: Direction,
    packet: Packet,
    length: u16,
    flag: Flag,
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

pub(crate) fn send(
    slot_id: SlotNumber,
    direction: Direction,
    packet: Packet,
    flag: Flag,
    payload: &[u8],
) -> bool {
    // FIXME: split into multiple messages if the payload is too large.
    if payload.len() > Header::payload_max_size() {
        panic!("Query too large");
    }
    let length = payload.len() as u16;
    let header = Header {
        direction: direction.clone(),
        packet,
        length,
        flag,
    };
    let header_bytes = header.as_bytes();
    let mut bus = Bus::new();
    let Some(slot) = bus.slot_locked(slot_id) else {
        return false;
    };
    let mut stream = SlotStream::from(slot);
    stream.write(header_bytes).expect("Failed to write header");
    stream.write(payload).expect("Failed to write payload");
    let slot = Slot::from(stream);
    slot.unlock();
    signal(slot_id, direction);
    true
}

pub(crate) fn read_header(stream: &mut SlotStream) -> Header {
    let mut buffer: [u8; Header::estimate_size()] = [0; Header::estimate_size()];
    stream.read_exact(&mut buffer).unwrap();
    Header::from_bytes(&buffer)
}
