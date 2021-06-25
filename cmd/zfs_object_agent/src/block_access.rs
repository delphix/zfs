use crate::base_types::DiskLocation;
use crate::base_types::Extent;
use anyhow::{anyhow, Result};
use log::*;
use metered::common::*;
use metered::hdr_histogram::AtomicHdrHistogram;
use metered::metered;
use metered::time_source::StdInstantMicros;
use more_asserts::*;
use nix::sys::stat::SFlag;
use num::Num;
use num::NumCast;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::os::unix::prelude::AsRawFd;
use std::time::Instant;
use tokio::fs::File;
use tokio::fs::OpenOptions;

//const MIN_SECTOR_SIZE: usize = 4 * 1024;
const MIN_SECTOR_SIZE: usize = 512;

#[derive(Serialize, Deserialize, Debug)]
struct BlockHeader {
    payload_size: usize,
    checksum: u64,
}

#[derive(Debug)]
pub struct BlockAccess {
    disk: File,
    size: u64,
    sector_size: usize,
    metrics: BlockAccessMetrics,
}

// Generate ioctl function
nix::ioctl_read!(ioctl_blkgetsize64, 0x12u8, 114u8, u64);
nix::ioctl_read_bad!(ioctl_blksszget, 0x1268, usize);

#[cfg(target_os = "linux")]
const CUSTOM_OFLAGS: i32 = libc::O_DIRECT;
#[cfg(not(target_os = "linux"))]
const CUSTOM_OFLAGS: i32 = 0;

// XXX this is very thread intensive.  On Linux, we can use "glommio" to use
// io_uring for much lower overheads.  Or SPDK (which can use io_uring or nvme
// hardware directly).  Or at least use O_DIRECT.
#[metered(registry=BlockAccessMetrics)]
impl BlockAccess {
    pub async fn new(disk_path: &str) -> BlockAccess {
        let disk = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(CUSTOM_OFLAGS)
            .open(disk_path)
            .await
            .unwrap();
        let stat = nix::sys::stat::fstat(disk.as_raw_fd()).unwrap();
        trace!("stat: {:?}", stat);
        let mode = SFlag::from_bits_truncate(stat.st_mode);
        let sector_size;
        let size;
        if mode.contains(SFlag::S_IFBLK) {
            size = unsafe {
                let mut cap: u64 = 0;
                let cap_ptr = &mut cap as *mut u64;
                ioctl_blkgetsize64(disk.as_raw_fd(), cap_ptr).unwrap();
                cap
            };
            sector_size = unsafe {
                let mut ssz: usize = 0;
                let ssz_ptr = &mut ssz as *mut usize;
                ioctl_blksszget(disk.as_raw_fd(), ssz_ptr).unwrap();
                ssz as usize
            };
            //sector_size = MIN_SECTOR_SIZE;
        } else if mode.contains(SFlag::S_IFREG) {
            size = stat.st_size as u64;
            sector_size = MIN_SECTOR_SIZE;
        } else {
            panic!("{}: invalid file type {:?}", disk_path, mode);
        }

        let this = BlockAccess {
            disk,
            size,
            sector_size,
            metrics: Default::default(),
        };
        info!("opening cache file {}: {:?}", disk_path, this);

        this
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn dump_metrics(&self) {
        debug!("metrics: {:#?}", self.metrics);
    }

    // offset and length must be sector-aligned
    // maybe this should return Bytes?
    #[measure(type = ResponseTime<AtomicHdrHistogram, StdInstantMicros>)]
    #[measure(InFlight)]
    #[measure(Throughput)]
    #[measure(HitCount)]
    pub async fn read_raw(&self, extent: Extent) -> Vec<u8> {
        assert_eq!(extent.size, self.round_up_to_sector(extent.size));
        let fd = self.disk.as_raw_fd();
        let sector_size = self.sector_size;
        let begin = Instant::now();
        let vec = tokio::task::spawn_blocking(move || {
            let mut v = Vec::new();
            // XXX use unsafe code to avoid double initializing it?
            // XXX directio requires the pointer to be sector-aligned, requiring this grossness
            v.resize(extent.size + sector_size, 0);
            let aligned = unsafe {
                let ptr = v.as_mut_ptr() as usize;
                let aligned_ptr = (ptr + sector_size - 1) / sector_size * sector_size;
                assert_le!(aligned_ptr - v.as_mut_ptr() as usize, sector_size);
                std::slice::from_raw_parts_mut(aligned_ptr as *mut u8, extent.size)
            };
            nix::sys::uio::pread(fd, aligned, extent.location.offset as i64).unwrap();
            // XXX copying again!
            aligned.to_owned()
        })
        .await
        .unwrap();
        trace!(
            "read({:?}) returned in {}us",
            extent,
            begin.elapsed().as_micros()
        );
        vec
    }

    // offset and data.len() must be sector-aligned
    // maybe this should take Bytes?
    #[measure(type = ResponseTime<AtomicHdrHistogram, StdInstantMicros>)]
    #[measure(InFlight)]
    #[measure(Throughput)]
    #[measure(HitCount)]
    pub async fn write_raw(&self, location: DiskLocation, data: Vec<u8>) {
        let fd = self.disk.as_raw_fd();
        let sector_size = self.sector_size;
        let length = data.len();
        let offset = location.offset;
        assert_eq!(offset, self.round_up_to_sector(offset));
        assert_eq!(length, self.round_up_to_sector(length));
        let begin = Instant::now();
        tokio::task::spawn_blocking(move || {
            let mut v = Vec::new();
            // XXX directio requires the pointer to be sector-aligned, requiring this grossness
            v.resize(length + sector_size, 0);
            let aligned = unsafe {
                let ptr = v.as_mut_ptr() as usize;
                let aligned_ptr = (ptr + sector_size - 1) / sector_size * sector_size;
                assert_le!(aligned_ptr - v.as_mut_ptr() as usize, sector_size);
                std::slice::from_raw_parts_mut(aligned_ptr as *mut u8, length)
            };
            // XXX copying
            aligned.copy_from_slice(&data);
            nix::sys::uio::pwrite(fd, aligned, offset as i64).unwrap();
        })
        .await
        .unwrap();
        trace!(
            "write({:?} len={}) returned in {}us",
            location,
            length,
            begin.elapsed().as_micros()
        );
    }

    pub fn round_up_to_sector<N: Num + NumCast + Copy>(&self, n: N) -> N {
        let sector_size: N = NumCast::from(self.sector_size).unwrap();
        (n + sector_size - N::one()) / sector_size * sector_size
    }

    // Given a logical write of this size, what will the raw size be?
    pub fn get_raw_size(&self, len: usize) -> usize {
        // XXX serialized size should be constant; could precomute it
        // Note: relies on bincode::config::FixintEncoding, which the serialize/deserialize functions use
        let header_size = bincode::serialized_size(&BlockHeader {
            payload_size: len,
            checksum: 0,
        })
        .unwrap() as usize;
        self.round_up_to_sector(header_size + len)
    }

    // XXX ideally this would return a sector-aligned address, so it can be used directly for a directio write
    pub fn json_chunk_to_raw<T: Serialize>(&self, struct_obj: &T) -> Vec<u8> {
        let payload = serde_json::to_vec(struct_obj).unwrap();
        let header = BlockHeader {
            payload_size: payload.len(),
            checksum: seahash::hash(&payload),
        };
        let header_bytes = bincode::serialize(&header).unwrap();
        let raw_len = self.round_up_to_sector(header_bytes.len() + payload.len());
        let mut tail: Vec<u8> = Vec::new();
        tail.resize(raw_len - header_bytes.len() - payload.len(), 0);
        // XXX copying data around; use serde_json::to_writer to append it into a larger-than-necessary vec?
        let buf = [header_bytes, payload, tail].concat();
        assert_eq!(buf.len(), raw_len);
        buf
    }

    /// returns deserialized struct and amount of the buf that was consumed
    pub fn json_chunk_from_raw<T: DeserializeOwned>(&self, buf: &[u8]) -> Result<(T, usize)> {
        let header: BlockHeader = bincode::deserialize(&buf).unwrap();
        let header_size = bincode::serialized_size(&header).unwrap() as usize;

        if header.payload_size as usize > buf.len() - header_size {
            return Err(anyhow!(
                "invalid length {}: expected at most {} bytes",
                header.payload_size,
                buf.len() - header_size
            ));
        }

        let data = &buf[header_size..header.payload_size + header_size];
        assert_eq!(data.len(), header.payload_size);
        let actual_checksum = seahash::hash(data);
        if header.checksum != actual_checksum {
            return Err(anyhow!(
                "incorrect checksum of {} bytes: expected {:x}, got {:x}",
                header.payload_size,
                header.checksum,
                actual_checksum,
            ));
        }

        let struct_obj: T = serde_json::from_slice(data)?;
        Ok((
            struct_obj,
            self.round_up_to_sector(header_size + data.len()),
        ))
    }
}
