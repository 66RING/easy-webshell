use crate::fs_opt::{FileInfo, create_zip_from_directory, DirectoryListing};
use crate::server::AppState;
use crate::api::types::FileQuery;
use axum::{
    response::{IntoResponse, Response},
    extract::State,
    body::Body,
};
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

/// Helper function to get current directory for a session
async fn get_current_dir(state: &AppState, session_id: Option<&String>) -> PathBuf {
    if let Some(sid) = session_id {
        let sessions = state.sessions.read().await;
        if let Some(dir) = sessions.get(sid) {
            return dir.lock().await.clone();
        }
    }
    state.initial_dir.clone()
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

/////////////// TODO: review

/// Index handler - serves the HTML page
pub async fn index_handler() -> impl IntoResponse {
    let html = get_html_content();
    axum::response::Html(html)
}


/// Download handler - serves file downloads
pub async fn download_handler(
    axum::extract::Query(params): axum::extract::Query<FileQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    use axum::http::StatusCode;

    // Get current directory based on session
    let current_dir = get_current_dir(&state, params.session_id.as_ref()).await;

    // Get path from query parameters
    let path_param = match params.path {
        Some(ref p) if !p.is_empty() => p.clone(),
        _ => return (StatusCode::BAD_REQUEST, "No file path specified").into_response(),
    };

    // URL decode the path
    let decoded_path = url_decoding(&path_param);

    // Build full file path
    let file_path = if decoded_path.starts_with('/') {
        PathBuf::from(&decoded_path)
    } else {
        current_dir.join(&decoded_path)
    };

    // Check if file exists
    if !file_path.exists() {
        return (StatusCode::NOT_FOUND, format!("File not found: {}", file_path.display())).into_response();
    }

    // Handle directory - create zip
    if file_path.is_dir() {
        info!("Zipping directory: {}", file_path.display());
        match create_zip_from_directory(&file_path) {
            Ok(zip_data) => {
                let dir_name = file_path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("archive");
                let filename = format!("{}.zip", dir_name);

                let response = Response::builder()
                    .status(200)
                    .header("content-type", "application/zip")
                    .header("content-disposition", format!("attachment; filename=\"{}\"", filename))
                    .header("content-length", zip_data.len())
                    .header("access-control-allow-origin", "*")
                    .body(Body::from(zip_data))
                    .unwrap();
                return response.into_response();
            }
            Err(e) => {
                return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
            }
        }
    }

    // Read regular file
    match fs::read(&file_path) {
        Ok(file_data) => {
            let content_type = mime_guess::from_path(&file_path)
                .first_or_octet_stream()
                .to_string();

            let filename = params.filename.unwrap_or_else(|| {
                file_path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("download")
                    .to_string()
            });

            info!("File downloaded: {} ({} bytes)", file_path.display(), file_data.len());

            let response = Response::builder()
                .status(200)
                .header("content-type", content_type)
                .header("content-disposition", format!("attachment; filename=\"{}\"", filename))
                .header("content-length", file_data.len())
                .header("access-control-allow-origin", "*")
                .body(Body::from(file_data))
                .unwrap();
            response.into_response()
        }
        Err(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

/// List handler - lists directory contents
pub async fn list_handler(
    axum::extract::Query(params): axum::extract::Query<FileQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    use axum::Json;
    use axum::http::StatusCode;

    let current_dir = get_current_dir(&state, params.session_id.as_ref()).await;

    // Get target path
    let target_path = if let Some(ref path) = params.path {
        if path.is_empty() || path == "." {
            current_dir.clone()
        } else {
            let decoded_path = url_decoding(path);
            if decoded_path.starts_with('/') {
                PathBuf::from(&decoded_path)
            } else {
                current_dir.join(&decoded_path)
            }
        }
    } else {
        current_dir.clone()
    };

    // Check if path exists
    if !target_path.exists() {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "Directory not found"}))).into_response();
    }

    let mut files = Vec::new();

    if target_path.is_dir() {
        // List directory contents
        match fs::read_dir(&target_path) {
            Ok(entries) => {
                for entry in entries.flatten() {
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
            Err(e) => {
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response();
            }
        }
    }

    // Sort: directories first, then files
    files.sort_by(|a, b| {
        if a.is_dir && !b.is_dir {
            std::cmp::Ordering::Less
        } else if !a.is_dir && b.is_dir {
            std::cmp::Ordering::Greater
        } else {
            a.name.cmp(&b.name)
        }
    });

    let listing = DirectoryListing {
        current_path: target_path.to_string_lossy().to_string(),
        files,
    };

    Json(listing).into_response()
}

/// Upload handler - handles file uploads
pub async fn upload_handler(
    State(state): State<AppState>,
    mut multipart: axum::extract::Multipart,
) -> impl IntoResponse {
    use axum::http::StatusCode;

    let current_dir = &state.initial_dir;

    // Process all fields until we find a file
    loop {
        let field_result: Result<Option<axum::extract::multipart::Field<'_>>, _> = multipart.next_field().await;

        let field = match field_result {
            Ok(Some(f)) => f,
            Ok(None) => break, // No more fields
            Err(_) => continue, // Skip error and try next field
        };

        // Get filename from the field
        let filename = match field.file_name() {
            Some(name) if !name.is_empty() => name.to_string(),
            _ => continue, // Skip fields without filename
        };

        // Get file data
        let data: Vec<u8> = match field.bytes().await {
            Ok(bytes) => bytes.to_vec(),
            Err(_) => return (StatusCode::BAD_REQUEST, "Failed to read file data").into_response(),
        };

        // Build file path
        let file_path = current_dir.join(&filename);

        // Create parent directories if needed
        if let Some(parent) = file_path.parent() {
            if !parent.exists() {
                if let Err(e) = fs::create_dir_all(parent) {
                    error!("Failed to create directory: {}", e);
                    return (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to create directory: {}", e)).into_response();
                }
            }
        }

        // Write file
        match File::create(&file_path) {
            Ok(mut file) => {
                if let Err(e) = file.write_all(&data) {
                    error!("Failed to write file: {}", e);
                    return (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to write file: {}", e)).into_response();
                }
                if let Err(e) = file.flush() {
                    error!("Failed to flush file: {}", e);
                    return (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to flush file: {}", e)).into_response();
                }

                info!("File uploaded: {} ({} bytes)", file_path.display(), data.len());

                return (StatusCode::OK, format!("File uploaded: {}", file_path.display())).into_response();
            }
            Err(e) => {
                error!("Failed to create file: {}", e);
                return (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to create file: {}", e)).into_response();
            }
        }
    }

    (StatusCode::BAD_REQUEST, "No valid file found in upload").into_response()
}

