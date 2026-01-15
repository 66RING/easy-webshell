use libc::{c_ushort, fcntl, ioctl, F_GETFL, F_SETFL, O_NONBLOCK, TIOCSWINSZ};
use pty::fork::{Fork, Master};
use std::env;
use std::ffi::CStr;
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::Command;

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
