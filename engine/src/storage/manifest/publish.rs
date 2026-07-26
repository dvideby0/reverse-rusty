//! CRC sealing and atomic publication shared by both manifest codecs.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

use super::super::{crc32, durable_rename, write_u32};

/// Publish one encoded manifest body as the sole durable commit point.
///
/// The codec is responsible only for writing its body. This boundary preserves
/// the required ordering: body fsync, read-back CRC, CRC fsync, then rename plus
/// parent-directory fsync.
pub(super) fn publish_with_crc(
    path: &Path,
    tmp: &Path,
    encode_body: impl FnOnce(&mut File) -> io::Result<()>,
) -> io::Result<()> {
    let mut file = File::create(tmp)?;
    encode_body(&mut file)?;
    file.sync_all()?;
    drop(file);

    let content = std::fs::read(tmp)?;
    let crc = crc32(&content);
    let mut file = OpenOptions::new().append(true).open(tmp)?;
    write_u32(&mut file, crc)?;
    file.sync_all()?;
    drop(file);

    durable_rename(tmp, path)
}
