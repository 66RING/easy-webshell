mod archive;

use serde::{Deserialize, Serialize};
pub use self::archive::create_zip_from_directory;

/// File information for directory listing
#[derive(Debug, Serialize, Deserialize)]
pub struct FileInfo {
    pub name: String,
    pub path: String,
    pub is_dir: bool,
    pub size: Option<u64>,
    pub modified: Option<u64>,
}

/// Directory listing response
#[derive(Debug, Serialize)]
pub struct DirectoryListing {
    pub current_path: String,
    pub files: Vec<FileInfo>,
}
