use libc::{c_ushort, fcntl, ioctl, F_GETFL, F_SETFL, O_NONBLOCK, TIOCSWINSZ};
use log::{debug, info};
use pty::fork::{Fork, Master};
use std::ffi::CStr;
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::{env, fs};
use tokio::sync::Mutex;

/// Terminal window size structure
#[repr(C)]
struct Winsize {
    ws_row: c_ushort,
    ws_col: c_ushort,
    ws_xpixel: c_ushort,
    ws_ypixel: c_ushort,
}

/// PTY session manager
pub struct PtySession {
    _fork: Fork,
    master: Option<Master>,
    pub pts_name: Option<String>,
}

impl PtySession {
    /// Create a new PTY session with specified size and initial directory
    /// TODO: review
    pub fn with_size_and_dir(
        cols: u16,
        rows: u16,
        initial_dir: Option<&PathBuf>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let shell = env::var("SHELL").unwrap_or_else(|_| "/bin/bash".to_string());

        let fork = Fork::from_ptmx()?;

        if let Ok(_child) = fork.is_child() {
            // Child process - set working directory if specified
            if let Some(dir) = initial_dir {
                if let Err(e) = env::set_current_dir(dir) {
                    eprintln!("Failed to set working directory {}: {}", dir.display(), e);
                }
            }

            // Spawn shell
            let _ = Command::new(&shell).exec();
            // If exec returns, there was an error
            Err(std::io::Error::new(std::io::ErrorKind::Other, "Failed to exec shell").into())
        } else {
            // Parent process
            let master = fork.is_parent()?;

            // Get the PTS slave name
            let pts_name = unsafe {
                master
                    .ptsname()
                    .ok()
                    .and_then(|s| CStr::from_ptr(s).to_str().ok())
                    .map(|s| s.to_string())
            };

            let mut session = PtySession {
                _fork: fork,
                master: Some(master.clone()),
                pts_name,
            };

            // Set master to non-blocking mode
            unsafe {
                let flags = fcntl(master.as_raw_fd(), F_GETFL, 0);
                if flags < 0 {
                    return Err(std::io::Error::last_os_error().into());
                }
                if fcntl(master.as_raw_fd(), F_SETFL, flags | O_NONBLOCK) < 0 {
                    return Err(std::io::Error::last_os_error().into());
                }
            }

            session.set_winsize(cols, rows)?;

            Ok(session)
        }
    }

    /// Set terminal window size
    pub fn set_winsize(&mut self, cols: u16, rows: u16) -> Result<(), std::io::Error> {
        if let Some(ref master) = self.master {
            let winsize = Winsize {
                ws_row: rows,
                ws_col: cols,
                ws_xpixel: 0,
                ws_ypixel: 0,
            };

            unsafe {
                if ioctl(master.as_raw_fd(), TIOCSWINSZ as u64, &winsize) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
            }
        }
        Ok(())
    }

    /// Read from PTY (non-blocking)
    pub fn read(&mut self, size: usize) -> Vec<u8> {
        if let Some(ref mut master) = self.master {
            let mut buffer = vec![0u8; size];
            match master.read(&mut buffer) {
                Ok(n) if n > 0 => {
                    buffer.truncate(n);
                    buffer
                }
                _ => Vec::new(),
            }
        } else {
            Vec::new()
        }
    }

    /// Write to PTY
    pub fn write(&mut self, data: &[u8]) {
        if let Some(ref mut master) = self.master {
            let _ = master.write_all(data);
        }
    }

    /// Close the PTY session
    pub fn close(&mut self) {
        if let Some(mut master) = self.master.take() {
            let _ = master.flush();
        }
    }
}

impl Drop for PtySession {
    fn drop(&mut self) {
        self.close();
    }
}

/// Sync current working directory from PTY
pub async fn sync_current_directory(
    pty_session: Arc<Mutex<PtySession>>,
    current_dir: Arc<Mutex<PathBuf>>,
) {
    // Get the PTS name from the session
    let pts_name = {
        let session = pty_session.lock().await;
        session.pts_name.clone()
    };

    if let Some(pts) = pts_name {
        // Find the process that has this PTY as its controlling terminal
        // by looking at /proc/[pid]/fd/0 (stdin) or /proc/[pid]/fd/1 (stdout)
        let cwd = find_shell_cwd(&pts);

        // Handle the result before any await
        let cwd_opt = cwd.ok();

        if let Some(cwd) = cwd_opt {
            let mut dir = current_dir.lock().await;
            let old_dir = dir.clone();
            *dir = cwd.clone();
            if old_dir != *dir {
                info!(
                    "Current directory updated: {} -> {}",
                    old_dir.display(),
                    dir.display()
                );
            }
        }
    }
}

/// Find the shell's current working directory by PTY device
/// by looking at /proc/[pid]/fd/0 (stdin) or /proc/[pid]/fd/1 (stdout)
/// if the fd_id was link to the target pts device. the process found.
///
/// Linux fs tips: every thing is a file.
/// process state: /proc/[pid]
///     opened file: /proc/[pid]/fd (a link to target file/device)
///     cwd: /proc/[pid]/cwd (a link)
///     ...
fn find_shell_cwd(pts_name: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let proc_path = PathBuf::from("/proc");

    // Iterate through all process directories in /proc
    for entry in fs::read_dir(&proc_path)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
    {
        let pid_str = entry.file_name();
        // Skip if not a numeric PID
        if pid_str
            .to_string_lossy()
            .chars()
            .all(|c| c.is_ascii_digit())
        {
            let pid: u32 = pid_str.to_string_lossy().parse().unwrap_or(0);
            if pid == 0 || pid == std::process::id() {
                continue;
            }

            // Check if this process has the PTY as its stdin/stdout/stderr
            // TODO: 新增一些pty的debug信息, 比如pts name
            let fds_path = entry.path().join("fd");
            if let Ok(fds) = fs::read_dir(&fds_path) {
                for fd_entry in fds.filter_map(|e| e.ok()) {
                    if let Ok(target) = fs::read_link(&fd_entry.path()) {
                        let target_str = target.to_string_lossy();
                        // Check if this fd points to our PTY
                        if target_str.contains(pts_name) || target_str == pts_name {
                            // Found the process! Now get its cwd
                            let cwd_path = entry.path().join("cwd");
                            if let Ok(cwd) = fs::read_link(&cwd_path) {
                                debug!("Found shell PID {} with cwd: {}", pid, cwd.display());
                                return Ok(cwd);
                            }
                        }
                    }
                }
            }
        }
    }

    // Fallback: return current directory
    debug!("Could not find shell process, using current directory");
    Ok(env::current_dir()?)
}
