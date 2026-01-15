use crate::fs_opt::{FileInfo, create_zip_from_directory, DirectoryListing};
use log::{error, info};
use std::fs::{self, File};
use std::io::Write;
use std::path::{PathBuf, Path};

/// HTML content for the terminal interface
pub fn get_html_content() -> String {
    std::fs::read_to_string("index.html").unwrap_or_else(|e| {
        error!("Failed to read index.html: {}", e);
        "<html><body><h1>Error loading page</h1></body></html>".to_string()
    })
}

/// Simple URL decoding (percent decoding)
pub fn url_decoding(input: &str) -> String {
    let mut result = String::new();
    let mut chars = input.chars();

    while let Some(c) = chars.next() {
        if c == '%' {
            let hex1 = chars.next();
            let hex2 = chars.next();

            if let (Some(h1), Some(h2)) = (hex1, hex2) {
                if let (Some(d1), Some(d2)) = (h1.to_digit(16), h2.to_digit(16)) {
                    let byte = (d1 * 16 + d2) as u8;
                    result.push(byte as char);
                } else {
                    result.push(c);
                    result.push(h1);
                    result.push(h2);
                }
            } else {
                result.push(c);
            }
        } else if c == '+' {
            result.push(' ');
        } else {
            result.push(c);
        }
    }

    result
}


/// Handle file download request
pub async fn handle_file_download(
    query: &str,
    current_dir: &Path,
) -> Result<(Vec<u8>, String), Box<dyn std::error::Error>> {
    // Parse query parameter: ?path=/filename or ?path=relative/path/file.txt or ?path=file.txt&session_id=xxx
    let path_param = query
        .strip_prefix("?")
        .unwrap_or(query)
        .split('&')
        .find_map(|p| {
            let p = p.trim_start_matches("path=");
            if p.contains('=') {
                None
            } else {
                Some(p)
            }
        })
        .unwrap_or("");

    if path_param.is_empty() {
        return Err("No file path specified".into());
    }

    // Decode URL encoding
    let decoded_path = url_decoding(path_param);

    // If path is absolute, use it directly; otherwise, join with current directory
    let file_path = if decoded_path.starts_with('/') {
        PathBuf::from(&decoded_path)
    } else {
        current_dir.join(&decoded_path)
    };

    if !file_path.exists() {
        return Err(format!("File not found: {}", file_path.display()).into());
    }

    // Check if it's a directory - if so, create a zip file
    if file_path.is_dir() {
        info!("Zipping directory: {}", file_path.display());
        let zip_data = create_zip_from_directory(&file_path)?;

        let dir_name = file_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("archive");

        let filename = format!("{}.zip", dir_name);

        // Determine content type for zip
        let content_type = "application/zip".to_string();

        info!(
            "Directory zipped: {} -> {} ({} bytes)",
            file_path.display(),
            filename,
            zip_data.len()
        );

        // Return zip with special filename header
        return Ok((zip_data, content_type));
    }

    // Read file content
    let file_data = fs::read(&file_path)?;

    // Determine content type
    let content_type = mime_guess::from_path(&file_path)
        .first_or_octet_stream()
        .to_string();

    info!(
        "File downloaded: {} ({} bytes)",
        file_path.display(),
        file_data.len()
    );

    Ok((file_data, content_type))
}


/// Parse multipart/form-data and extract file
fn parse_multipart_upload(
    body: &[u8],
    boundary: &str,
) -> Result<(String, Vec<u8>), Box<dyn std::error::Error>> {
    let boundary_str = format!("--{}", boundary);
    let boundary_bytes = boundary_str.as_bytes();

    let mut start = 0;

    // Find each part
    let mut filename = String::new();
    let mut file_data = Vec::new();

    while start < body.len() {
        // Find boundary
        let boundary_pos = body[start..]
            .windows(boundary_bytes.len())
            .position(|w| w == boundary_bytes);

        let boundary_pos = match boundary_pos {
            Some(pos) => pos + start,
            None => break,
        };

        // Check if this is the end boundary
        let end_marker_start = boundary_pos + boundary_bytes.len();
        if end_marker_start + 2 <= body.len()
            && &body[end_marker_start..end_marker_start + 2] == b"--"
        {
            break;
        }

        // Find end of headers (double newline)
        let headers_end = body[boundary_pos + boundary_bytes.len() + 2..]
            .windows(4)
            .position(|w| w == b"\r\n\r\n");

        let headers_end = match headers_end {
            Some(pos) => pos + boundary_pos + boundary_bytes.len() + 2,
            None => break,
        };

        // Parse headers to find filename (only headers are text, data is binary)
        let headers_section =
            String::from_utf8_lossy(&body[boundary_pos + boundary_bytes.len() + 2..headers_end]);
        let data_start = headers_end + 4;

        // Extract filename from Content-Disposition header
        for line in headers_section.lines() {
            if line.contains("filename=") {
                let start = line.find("filename=\"").unwrap() + 10;
                let end = line[start..].find('"').unwrap();
                filename = line[start..start + end].to_string();
                // Normalize path separators to forward slash
                filename = filename.replace('\\', "/");
                break;
            }
        }

        // Find next boundary
        let next_boundary = body[data_start..]
            .windows(boundary_bytes.len())
            .position(|w| w == boundary_bytes);

        let data_end = match next_boundary {
            Some(pos) => pos + data_start - 2, // -2 for \r\n before boundary
            None => body.len(),
        };

        if !filename.is_empty() {
            // Copy raw bytes for binary data
            file_data = body[data_start..data_end].to_vec();
            break;
        }

        start = boundary_pos + 1;
    }

    if filename.is_empty() {
        return Err("No file found in upload".into());
    }

    Ok((filename, file_data))
}

/// Handle file upload request
pub async fn handle_file_upload(
    content_type: &str,
    body: &[u8],
    current_dir: &Path,
) -> Result<String, Box<dyn std::error::Error>> {
    // Extract boundary from Content-Type
    let boundary = content_type
        .strip_prefix("multipart/form-data; boundary=")
        .ok_or("Invalid content type")?;

    let (filename, file_data) = parse_multipart_upload(body, boundary)?;

    let file_path = current_dir.join(&filename);

    // Create parent directories if they don't exist
    if let Some(parent) = file_path.parent() {
        if !parent.exists() {
            fs::create_dir_all(parent)?;
        }
    }

    // Write file to disk (binary mode)
    let mut file = File::create(&file_path)?;
    file.write_all(&file_data)?;
    file.flush()?;

    info!(
        "File uploaded: {} ({} bytes)",
        file_path.display(),
        file_data.len()
    );

    Ok(format!("File uploaded: {}", file_path.display()))
}

/// List files in a directory
pub async fn handle_list_directory(
    query: &str,
    current_dir: &Path,
) -> Result<DirectoryListing, Box<dyn std::error::Error>> {
    // Parse query parameter: ?path=/folder or ?path=relative/path or ?path=.&session_id=xxx
    let path_param = query
        .strip_prefix("?")
        .unwrap_or(query)
        .split('&')
        .find_map(|p| {
            let p = p.trim_start_matches("path=");
            if p.contains('=') {
                None
            } else {
                Some(p)
            }
        })
        .unwrap_or("");

    let target_path = if path_param.is_empty() || path_param == "." {
        current_dir.to_path_buf()
    } else {
        let decoded_path = url_decoding(path_param);
        if decoded_path.starts_with('/') {
            PathBuf::from(&decoded_path)
        } else {
            current_dir.join(&decoded_path)
        }
    };

    if !target_path.exists() {
        return Err("Directory not found".into());
    }

    let mut files = Vec::new();

    if target_path.is_dir() {
        // List directory contents
        let entries = fs::read_dir(&target_path)?;
        for entry in entries {
            let entry = entry?;
            let metadata = entry.metadata().ok();
            let name = entry.file_name().to_string_lossy().to_string();
            let is_dir = metadata.as_ref().map(|m| m.is_dir()).unwrap_or(false);
            let size = if !is_dir {
                metadata.as_ref().map(|m| m.len())
            } else {
                None
            };
            let modified = metadata.as_ref().and_then(|m| m.modified().ok()).map(|t| {
                t.duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            });

            // Skip hidden files
            if !name.starts_with('.') {
                files.push(FileInfo {
                    path: entry.path().to_string_lossy().to_string(),
                    name,
                    is_dir,
                    size,
                    modified,
                });
            }
        }
    }

    // Sort: directories first, then files
    files.sort_by(|a, b| {
        if a.is_dir && !b.is_dir {
            return std::cmp::Ordering::Less;
        } else if !a.is_dir && b.is_dir {
            return std::cmp::Ordering::Greater;
        }
        a.name.cmp(&b.name)
    });

    Ok(DirectoryListing {
        current_path: target_path.to_string_lossy().to_string(),
        files,
    })
}
