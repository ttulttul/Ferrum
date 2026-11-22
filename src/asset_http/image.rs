// Only inspect the first 256 KiB, including JPEG EXIF orientation, as the client does.
use serde_json::{json, Value};
use std::{fs::File, io::Read, path::Path};

pub(super) fn image_dimensions(path: &Path) -> Option<Value> {
    let mut bytes = Vec::new();
    File::open(path)
        .ok()?
        .take(256 * 1024)
        .read_to_end(&mut bytes)
        .ok()?;
    let (width, height) = dimensions(&bytes)?;
    (width > 0 && height > 0).then(|| json!({ "width": width, "height": height }))
}

fn dimensions(b: &[u8]) -> Option<(u32, u32)> {
    let word = |i| Some(u16::from_le_bytes(b.get(i..i + 2)?.try_into().ok()?) as u32);
    let uint = |i| b.get(i..i + 4)?.try_into().ok();
    if b.starts_with(b"\x89PNG") && b.get(12..16) == Some(b"IHDR") {
        return Some((u32::from_be_bytes(uint(16)?), u32::from_be_bytes(uint(20)?)));
    }
    if b.starts_with(b"GIF8") {
        return Some((word(6)?, word(8)?));
    }
    if b.starts_with(b"RIFF") && b.get(8..12) == Some(b"WEBP") {
        return match b.get(12..16)? {
            b"VP8 " => Some((word(26)? & 0x3fff, word(28)? & 0x3fff)),
            b"VP8L" => {
                let v = u32::from_le_bytes(uint(21)?);
                Some(((v & 0x3fff) + 1, ((v >> 14) & 0x3fff) + 1))
            }
            b"VP8X" => Some((
                1 + word(24)? + ((*b.get(26)? as u32) << 16),
                1 + word(27)? + ((*b.get(29)? as u32) << 16),
            )),
            _ => None,
        };
    }
    if !b.starts_with(&[0xff, 0xd8]) {
        return None;
    }
    let mut i = 2;
    let mut rotated = false;
    while i + 9 <= b.len() && b[i] == 0xff {
        let marker = b[i + 1];
        match marker {
            0xff => i += 1,
            0xd9 | 0xda => break,
            0x01 | 0xd0..=0xd7 => i += 2,
            0xc0..=0xcf if !matches!(marker, 0xc4 | 0xc8 | 0xcc) => {
                let height = u16::from_be_bytes([b[i + 5], b[i + 6]]) as u32;
                let width = u16::from_be_bytes([b[i + 7], b[i + 8]]) as u32;
                return Some(if rotated {
                    (height, width)
                } else {
                    (width, height)
                });
            }
            _ => {
                let end = i + 2 + u16::from_be_bytes([b[i + 2], b[i + 3]]) as usize;
                if marker == 0xe1 {
                    rotated |= b
                        .get(i + 4..end.min(b.len()))
                        .and_then(exif_orientation)
                        .is_some_and(|v| (5..=8).contains(&v));
                }
                i = end;
            }
        }
    }
    None
}

fn exif_orientation(segment: &[u8]) -> Option<u16> {
    if !segment.starts_with(b"Exif") {
        return None;
    }
    let tiff = segment.get(6..)?;
    let little = match tiff.get(..2)? {
        b"II" => true,
        b"MM" => false,
        _ => return None,
    };
    let short = |bytes| {
        if little {
            u16::from_le_bytes(bytes)
        } else {
            u16::from_be_bytes(bytes)
        }
    };
    let offset = tiff.get(4..8)?.try_into().ok()?;
    let offset = if little {
        u32::from_le_bytes(offset)
    } else {
        u32::from_be_bytes(offset)
    };
    let ifd = tiff.get(offset as usize..)?;
    let entries = short(ifd.get(..2)?.try_into().ok()?);
    for entry in ifd[2..].chunks_exact(12).take(entries.into()) {
        if short([entry[0], entry[1]]) == 0x0112 {
            return Some(short([entry[8], entry[9]]));
        }
    }
    None
}
