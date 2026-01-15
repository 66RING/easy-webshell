use std::fs::{self, File};
use std::io::{Cursor, Read, Write};
use std::path::Path;

use zip::write::FileOptions;
use zip::ZipWriter;

/// Create a zip file from a directory
pub fn create_zip_from_directory(dir_path: &Path) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut buffer = Vec::new();
    let mut zip = ZipWriter::new(Cursor::new(&mut buffer));

    fn add_to_zip(
        zip: &mut ZipWriter<Cursor<&mut Vec<u8>>>,
        dir: &Path,
        base: &Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let options: FileOptions<'_, ()> = FileOptions::default();

        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let name = path.strip_prefix(base)?.to_string_lossy().to_string();

            if path.is_dir() {
                zip.add_directory(name.clone(), options)?;
                add_to_zip(zip, &path, base)?;
            } else {
                zip.start_file(name, options)?;
                let mut file = File::open(&path)?;
                let mut file_buffer = Vec::new();
                file.read_to_end(&mut file_buffer)?;
                zip.write_all(&file_buffer)?;
            }
        }
        Ok(())
    }

    let base = dir_path.parent().unwrap_or_else(|| Path::new("."));
    add_to_zip(&mut zip, dir_path, base)?;
    zip.finish()?;

    Ok(buffer)
}
