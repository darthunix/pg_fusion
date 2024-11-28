use crate::ipc::{Bus, Slot, SlotNumber, DATA_SIZE};
use pgrx::prelude::*;
use std::io::Write;
use std::slice::from_raw_parts;

#[repr(C)]
pub(crate) enum Direction {
    ToWorker = 0,
    FromWorker = 1,
}

#[repr(C)]
pub(crate) enum Packet {
    Ack = 0,
    Query = 1,
}

#[repr(C)]
pub(crate) enum Flag {
    More = 0,
    Last = 1,
}

#[repr(C)]
pub(crate) struct Header {
    direction: Direction,
    packet: Packet,
    length: u16,
    flags: Flag,
}

impl Header {
    const fn estimate_size() -> usize {
        std::mem::size_of::<Self>()
    }

    const fn payload_max_size() -> usize {
        DATA_SIZE - Self::estimate_size()
    }

    fn as_bytes(&self) -> &[u8] {
        unsafe { from_raw_parts(self as *const Self as *const u8, Self::estimate_size()) }
    }
}

pub(crate) fn send(
    slot_id: SlotNumber,
    direction: Direction,
    packet: Packet,
    flags: Flag,
    payload: &[u8],
) -> bool {
    // FIXME: split into multiple messages if the payload is too large.
    if payload.len() > Header::payload_max_size() {
        panic!("Query too large");
    }
    let length = payload.len() as u16;
    let header = Header {
        direction,
        packet,
        length,
        flags,
    };
    let header_bytes = header.as_bytes();
    let mut bus = Bus::new();
    let Some(mut slot) = bus.slot(slot_id) else {
        return false;
    };
    slot.write(header_bytes).expect("Failed to write header");
    slot.write(payload).expect("Failed to write payload");
    slot.unlock();
    true
}
