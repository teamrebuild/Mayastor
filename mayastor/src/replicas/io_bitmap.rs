use derivative::Derivative;
use std::{thread::sleep, time::Duration};

// Size per bit is set to 64KiB to match SPDK's max I/O size
const SIZE_PER_BIT: u64 = 64 * 1024;
const BITS_PER_ENTRY: u64 = 64;
const ENTRY_SIZE: u64 = SIZE_PER_BIT * BITS_PER_ENTRY;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct IoBitmap {
    volume_size: u64,
    blk_size: u64,
    bitmap_size: u64,
    #[derivative(Debug = "ignore")]
    in_use_map: atomic_array::AtomicIsizeArray,
}

impl IoBitmap {
    pub fn new(volume_size: u64) -> Self {
        println!("NEW BITMAP");
        let mut bitmap_size = volume_size / ENTRY_SIZE;
        if bitmap_size == 0 {
            bitmap_size += 1;
        }
        println!(
            "bitmap_size: {}, volume_size: {}, entry_size: {}",
            bitmap_size, volume_size, ENTRY_SIZE
        );
        Self {
            volume_size,
            blk_size: 0,
            bitmap_size,
            in_use_map: atomic_array::AtomicIsizeArray::new(
                bitmap_size as usize,
            ),
        }
    }

    pub fn set_blk_size(&mut self, size: u32) {
        println!("BITMAP: set blk size to {}:{}", size, size as u64);
        self.blk_size = size as u64;
    }

    // Set the bits corresponding to the address range.
    // This marks the segments as "in use".
    pub fn set(&self, offset: u64, len: u64) {
        let start_addr = self.get_bit_addr(offset);
        let end_addr = self.get_bit_addr(offset + len);
        assert!(end_addr - start_addr <= SIZE_PER_BIT);

        println!(
            "SET => offset: {}, len: {}, start: {}, end: {}",
            offset, len, start_addr, end_addr
        );

        let idx = start_addr / ENTRY_SIZE;
        let idx_start_addr = idx * ENTRY_SIZE;
        let bit_offset = (start_addr - idx_start_addr) / SIZE_PER_BIT;
        println!(
            "SET => idx: {}, idx_start_addr: {}, bit_offset: {}",
            idx, idx_start_addr, bit_offset
        );

        // Continually try to set the bit
        loop {
            let old = self.in_use_map.load(idx as usize) as u64;
            // TODO: check if the segment is already set and wait on it

            let new = old | (1u64 << bit_offset);
            if self.in_use_map.swap(idx as usize, new as isize) == old as isize
            {
                break;
            }
            sleep(Duration::from_millis(10));
            println!("Set loop");
        }
    }

    // Clear the bits corresponding to the address range.
    // This marks them as available for use.
    pub fn clear(&self, offset: u64, len: u64) {
        let start_addr = self.get_bit_addr(offset);
        let end_addr = self.get_bit_addr(offset + len);
        assert!(end_addr - start_addr <= SIZE_PER_BIT);
        println!(
            "CLEAR => offset: {}, len: {}, start: {}, end: {}, blk_size: {}",
            offset, len, start_addr, end_addr, self.blk_size
        );

        let idx = start_addr / ENTRY_SIZE;
        let idx_start_addr = idx * ENTRY_SIZE;
        let bit_offset = (start_addr - idx_start_addr) / SIZE_PER_BIT;
        println!(
            "CLEAR => idx: {}, idx_start_addr: {}, bit_offset: {}",
            idx, idx_start_addr, bit_offset
        );

        // Continually try to clear the bit
        loop {
            let old = self.in_use_map.load(idx as usize) as u64;
            let new = old | !(1u64 << bit_offset);
            if self.in_use_map.swap(idx as usize, new as isize) == old as isize
            {
                break;
            }
            sleep(Duration::from_millis(10));
            println!("Clear loop");
        }
    }

    fn get_bit_addr(&self, block: u64) -> u64 {
        block * self.blk_size
    }
}
