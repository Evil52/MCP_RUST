//! Decode bounded CSV archives in memory; never extract upstream paths to disk.
use super::WbError;
use serde_json::{Value, json};
use std::io::{Cursor, Read};

const MAX_CSV_BYTES: u64 = 2 * 1024 * 1024;
const MAX_CSV_LIMIT: usize = 2 * 1024 * 1024;
const MAX_FILES: usize = 4;

pub(super) fn decode(bytes: Vec<u8>, request_id: Option<String>) -> Result<Value, WbError> {
    let invalid = || WbError::InvalidJson {
        request_id: request_id.clone(),
        source: serde::de::Error::custom(
            "WB report must be a bounded ZIP archive of UTF-8 CSV files",
        ),
    };
    let mut archive = zip::ZipArchive::new(Cursor::new(bytes)).map_err(|_| invalid())?;
    if archive.is_empty() || archive.len() > MAX_FILES {
        return Err(invalid());
    }
    let mut total = 0_u64;
    let mut actual_total = 0_u64;
    let mut files = Vec::new();
    for i in 0..archive.len() {
        let file = archive.by_index(i).map_err(|_| invalid())?;
        if file.is_dir()
            || !file.name().to_ascii_lowercase().ends_with(".csv")
            || file.enclosed_name().is_none()
        {
            return Err(invalid());
        }
        total = total.checked_add(file.size()).ok_or_else(invalid)?;
        if total > MAX_CSV_BYTES {
            return Err(WbError::ResponseTooLarge {
                limit_bytes: MAX_CSV_LIMIT,
                actual_bytes: Some(total),
                request_id,
            });
        }
        let name = file.name().to_owned();
        let mut csv = String::new();
        file.take(MAX_CSV_BYTES.saturating_sub(actual_total) + 1)
            .read_to_string(&mut csv)
            .map_err(|_| invalid())?;
        actual_total += csv.len() as u64;
        if actual_total > MAX_CSV_BYTES {
            return Err(invalid());
        }
        files.push(json!({"name": name, "csv": csv}));
    }
    Ok(json!({"format": "csv", "files": files, "truncated": false}))
}
