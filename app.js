(function() {
    'use strict';

    // ==================== Private State ====================
    let term = null;
    let ws = null;
    let wsConnected = false;
    let authenticated = false;
    let sessionId = null;
    let authToken = null;
    let pendingInput = [];
    let pendingAuth = false;
    let currentBrowserPath = '.';
    let selectedFile = null;

    // ==================== Private Functions ====================
    function getCellDimensions() {
        return { width: 8.4, height: 18 };
    }

    function fitTerminal() {
        if (!term) return;
        const container = document.getElementById('terminal');
        const width = container.clientWidth;
        const height = container.clientHeight;
        const cell = getCellDimensions();
        const cols = Math.max(1, Math.floor((width - 8) / cell.width));
        const rows = Math.max(1, Math.floor((height - 8) / cell.height));
        console.log(`Terminal size: ${cols}x${rows}, container: ${width}x${height}`);
        term.resize(cols, rows);
    }

    function sendToWebSocket(data) {
        if (wsConnected && authenticated && ws) {
            ws.send(data);
            return true;
        } else if (wsConnected && ws) {
            pendingInput.push(data);
            return false;
        }
        return false;
    }

    function showLoginModal() {
        document.getElementById('login-modal').classList.add('active');
        document.getElementById('username').focus();
    }

    function hideLoginModal() {
        document.getElementById('login-modal').classList.remove('active');
    }

    function showLoginError(message) {
        const errorDiv = document.getElementById('login-error');
        errorDiv.textContent = message;
        errorDiv.style.display = 'block';
    }

    function hideLoginError() {
        document.getElementById('login-error').style.display = 'none';
    }

    function openFileBrowser(path) {
        if (!authenticated || !authToken) {
            console.warn('Not authenticated');
            return;
        }
        selectedFile = null;
        document.getElementById('file-browser-modal').classList.add('active');
        document.getElementById('download-selected-btn').disabled = true;
        loadDirectory(path);
    }

    function closeFileBrowser() {
        document.getElementById('file-browser-modal').classList.remove('active');
        selectedFile = null;
    }

    async function loadDirectory(path) {
        if (!authenticated || !authToken) {
            console.warn('Not authenticated');
            return;
        }

        const fileList = document.getElementById('file-list');
        const pathInput = document.getElementById('current-path-input');
        pathInput.value = path;
        fileList.innerHTML = '<div class="loading-spinner">Loading...</div>';

        try {
            const encodedPath = encodeURIComponent(path);
            const sessionParam = `&session_id=${encodeURIComponent(sessionId)}`;
            const tokenParam = `&token=${encodeURIComponent(authToken)}`;
            const response = await fetch(`/api/ls?path=${encodedPath}${tokenParam}${sessionParam}`);

            if (response.ok) {
                const data = await response.json();
                pathInput.value = data.current_path;
                currentBrowserPath = data.current_path;
                renderFileList(data.files);
            } else {
                fileList.innerHTML = '<div class="loading-spinner">Failed to load directory</div>';
            }
        } catch (error) {
            fileList.innerHTML = '<div class="loading-spinner">Error: ' + error.message + '</div>';
        }
    }

    function renderFileList(files) {
        const fileList = document.getElementById('file-list');
        fileList.innerHTML = '';

        if (files.length === 0) {
            fileList.innerHTML = '<div class="loading-spinner">Directory is empty</div>';
            return;
        }

        if (currentBrowserPath !== '.' && currentBrowserPath !== '/') {
            const parentItem = document.createElement('div');
            parentItem.className = 'file-item directory';
            parentItem.innerHTML = '<span class="file-item-icon">📁</span><span class="file-item-name">.. (parent)</span>';
            parentItem.onclick = () => {
                let parentPath;
                if (currentBrowserPath.includes('/')) {
                    const parts = currentBrowserPath.split('/');
                    parts.pop();
                    parentPath = parts.join('/') || '/';
                } else {
                    const parts = currentBrowserPath.split('/');
                    parts.pop();
                    parentPath = parts.join('/') || '.';
                }
                openFileBrowser(parentPath);
            };
            fileList.appendChild(parentItem);
        }

        files.forEach(file => {
            const item = document.createElement('div');
            item.className = `file-item ${file.is_dir ? 'directory' : 'file'}`;
            const icon = file.is_dir ? '📁' : '📄';
            const size = file.size ? formatFileSize(file.size) : '';
            item.innerHTML = `
                <span class="file-item-icon">${icon}</span>
                <span class="file-item-name">${file.name}</span>
                ${size ? `<span class="file-item-size">${size}</span>` : ''}
            `;

            item.onclick = () => {
                document.querySelectorAll('.file-item').forEach(el => el.style.background = '');
                item.style.background = '#2472c8';
                selectedFile = file;
                document.getElementById('download-selected-btn').disabled = false;
            };

            item.ondblclick = () => {
                if (file.is_dir) {
                    let newPath;
                    if (currentBrowserPath === '.') {
                        newPath = file.name;
                    } else {
                        newPath = currentBrowserPath + '/' + file.name;
                    }
                    openFileBrowser(newPath);
                }
            };

            fileList.appendChild(item);
        });
    }

    function formatFileSize(bytes) {
        if (bytes < 1024) return bytes + ' B';
        if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
        if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
        return (bytes / (1024 * 1024 * 1024)).toFixed(1) + ' GB';
    }

    async function uploadFile(file, path, maxRetries = 3) {
        if (!authenticated || !authToken) {
            console.warn('Not authenticated');
            return false;
        }

        const formData = new FormData();
        const blob = file instanceof Blob ? file : new Blob([file], { type: file.type || 'application/octet-stream' });
        formData.append('file', blob, path);

        const sessionParam = `&session_id=${encodeURIComponent(sessionId)}`;
        const tokenParam = `?token=${encodeURIComponent(authToken)}`;

        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                const response = await fetch(`/api/upload${tokenParam}${sessionParam}`, {
                    method: 'POST',
                    body: formData
                });

                if (response.ok) {
                    const result = await response.text();
                    term.write('  \x1b[32m✓\x1b[0m ' + result + '\r\n');
                    return true;
                } else {
                    const errorText = await response.text();
                    if (attempt === maxRetries) {
                        term.write('  \x1b[31m✗\x1b[0m Failed: ' + errorText + '\r\n');
                    }
                }
            } catch (error) {
                if (attempt === maxRetries) {
                    term.write('  \x1b[31m✗\x1b[0m Network error: ' + error.message + '\r\n');
                } else {
                    await new Promise(resolve => setTimeout(resolve, 1000 * attempt));
                }
            }
        }
        return false;
    }

    async function traverseFileTree(item, path, files) {
        if (item.isFile) {
            return new Promise((resolve) => {
                item.file((file) => {
                    files.push({ file, path: path + file.name });
                    resolve();
                });
            });
        } else if (item.isDirectory) {
            const dirReader = item.createReader();
            const entries = await new Promise((resolve) => {
                dirReader.readEntries(resolve);
            });
            for (const entry of entries) {
                await traverseFileTree(entry, path + item.name + '/', files);
            }
        }
    }

    async function downloadFile(path, isDir = false) {
        if (!authenticated || !authToken) {
            console.warn('Not authenticated');
            return;
        }

        try {
            term.write('\r\n\x1b[33mDownloading: ' + path + (isDir ? ' (as ZIP)' : '') + '\x1b[0m\r\n');

            const encodedPath = encodeURIComponent(path);
            const tokenParam = `&token=${encodeURIComponent(authToken)}`;
            const sessionParam = `&session_id=${encodeURIComponent(sessionId)}`;
            const response = await fetch(`/api/download?path=${encodedPath}${tokenParam}${sessionParam}`);

            if (response.ok) {
                const contentDisposition = response.headers.get('Content-Disposition');
                let filename = path.split('/').pop();

                if (contentDisposition) {
                    const filenameMatch = contentDisposition.match(/filename="(.+)"/);
                    if (filenameMatch) {
                        filename = filenameMatch[1];
                    }
                }

                const blob = await response.blob();
                const url = window.URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = filename;
                document.body.appendChild(a);
                a.click();
                document.body.removeChild(a);
                window.URL.revokeObjectURL(url);

                term.write('\r\n\x1b[32m✓ Downloaded: ' + filename + '\x1b[0m\r\n');
            } else {
                const errorText = await response.text();
                term.write('\r\n\x1b[31m✗ Download failed: ' + errorText + '\x1b[0m\r\n');
            }
        } catch (error) {
            term.write('\r\n\x1b[31m✗ Download error: ' + error.message + '\x1b[0m\r\n');
        }
    }

    // ==================== Initialization ====================
    function initialize() {
        // Create xterm.js instance
        term = new Terminal({
            cursorBlink: true,
            fontSize: 14,
            fontFamily: 'Consolas, "Courier New", monospace',
            theme: {
                background: '#1e1e1e',
                foreground: '#ffffff',
                cursor: '#ffffff',
                black: '#000000',
                red: '#cd3131',
                green: '#0dbc79',
                yellow: '#e5e510',
                blue: '#2472c8',
                magenta: '#bc3fbc',
                cyan: '#11a8cd',
                white: '#e5e5e5',
                brightBlack: '#666666',
                brightRed: '#f14c4c',
                brightGreen: '#23d18b',
                brightYellow: '#f5f543',
                brightBlue: '#3b8eea',
                brightMagenta: '#d670d6',
                brightCyan: '#29b8db',
                brightWhite: '#ffffff'
            }
        });

        // Mount terminal to DOM
        term.open(document.getElementById('terminal'));

        // Initial fit
        fitTerminal();
        window.addEventListener('resize', () => {
            fitTerminal();
            if (wsConnected && term) {
                const resize = { cols: term.cols, rows: term.rows };
                ws.send(JSON.stringify(resize));
            }
        });

        // Connect to WebSocket
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        ws = new WebSocket(`${protocol}//${window.location.host}/ws`);

        ws.onopen = () => {
            console.log('WebSocket connected');
            wsConnected = true;
        };

        ws.onmessage = (event) => {
            try {
                const msg = JSON.parse(event.data);
                if (msg.auth === 'required') {
                    if (msg.session_id) {
                        sessionId = msg.session_id;
                        console.log('Session ID (auth required):', sessionId);
                    }
                    if (!authenticated && !pendingAuth) {
                        showLoginModal();
                    }
                    return;
                } else if (msg.auth === 'success') {
                    authenticated = true;
                    pendingAuth = false;

                    if (msg.session_id) {
                        sessionId = msg.session_id;
                        console.log('Session ID (auth success):', sessionId);
                    }

                    if (msg.token) {
                        authToken = msg.token;
                        console.log('Auth token received');
                    }

                    const loginBtn = document.querySelector('.btn-login');
                    loginBtn.disabled = false;
                    loginBtn.textContent = 'Login';

                    hideLoginModal();
                    fitTerminal();

                    const resize = { cols: term.cols, rows: term.rows };
                    ws.send(JSON.stringify(resize));

                    while (pendingInput.length > 0) {
                        const data = pendingInput.shift();
                        ws.send(data);
                    }
                    return;
                } else if (msg.auth === 'failed') {
                    pendingAuth = false;
                    showLoginError('Invalid username or password');

                    const loginBtn = document.querySelector('.btn-login');
                    loginBtn.disabled = false;
                    loginBtn.textContent = 'Login';

                    document.getElementById('password').value = '';
                    document.getElementById('password').focus();
                    return;
                }
            } catch (e) {
                // Not JSON, treat as regular terminal data
            }

            if (authenticated) {
                term.write(event.data);
                term.scrollToBottom();
            }
        };

        ws.onclose = () => {
            console.log('WebSocket disconnected');
            wsConnected = false;
            authenticated = false;
            pendingAuth = false;

            if (document.getElementById('login-modal').classList.contains('active') || !authenticated) {
                showLoginModal();
                showLoginError('Connection lost. Please refresh the page to reconnect.');
                const loginBtn = document.querySelector('.btn-login');
                loginBtn.disabled = true;
                loginBtn.textContent = 'Disconnected';
            } else {
                term.write('\r\n\x1b[31mConnection closed\x1b[0m');
            }
        };

        ws.onerror = (error) => {
            console.error('WebSocket error:', error);
        };

        // Terminal input
        term.onData(data => {
            sendToWebSocket(data);
        });

        // Login form
        document.getElementById('login-form').addEventListener('submit', (e) => {
            e.preventDefault();

            const username = document.getElementById('username').value;
            const password = document.getElementById('password').value;
            const loginBtn = document.querySelector('.btn-login');

            if (!username || !password) {
                showLoginError('Please enter both username and password');
                return;
            }

            loginBtn.disabled = true;
            loginBtn.textContent = 'Logging in...';

            pendingAuth = true;
            hideLoginError();

            ws.send(JSON.stringify({
                auth: 'login',
                username: username,
                password: password
            }));

            setTimeout(() => {
                if (pendingAuth) {
                    loginBtn.disabled = false;
                    loginBtn.textContent = 'Login';
                    showLoginError('Connection timeout. Please try again.');
                    pendingAuth = false;
                }
            }, 10000);
        });

        // Drag and drop
        const dropOverlay = document.getElementById('drop-overlay');

        ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
            document.body.addEventListener(eventName, (e) => {
                e.preventDefault();
                e.stopPropagation();
            }, false);
        });

        ['dragenter', 'dragover'].forEach(eventName => {
            document.body.addEventListener(eventName, () => {
                dropOverlay.classList.add('active');
            }, false);
        });

        ['dragleave', 'drop'].forEach(eventName => {
            document.body.addEventListener(eventName, () => {
                dropOverlay.classList.remove('active');
            }, false);
        });

        document.body.addEventListener('drop', async (e) => {
            if (!authenticated) {
                console.warn('Not authenticated');
                return;
            }

            const dt = e.dataTransfer;
            const items = dt.items;

            if (items) {
                const files = [];
                for (let i = 0; i < items.length; i++) {
                    const item = items[i].webkitGetAsEntry();
                    if (item) {
                        await traverseFileTree(item, '', files);
                    }
                }
                term.write('\r\n\x1b[33mPreparing to upload ' + files.length + ' files...\x1b[0m\r\n');
                let successCount = 0;
                let failCount = 0;
                for (let i = 0; i < files.length; i++) {
                    const fileData = files[i];
                    term.write(`\x1b[90m[${i + 1}/${files.length}]\x1b[0m Uploading: ${fileData.path}\r\n`);
                    const success = await uploadFile(fileData.file, fileData.path);
                    if (success) successCount++;
                    else failCount++;
                }
                term.write('\r\n\x1b[36mUpload complete: ' + successCount + ' succeeded, ' + failCount + ' failed\x1b[0m\r\n');
            } else {
                const files = dt.files;
                term.write('\r\n\x1b[33mUploading ' + files.length + ' files...\x1b[0m\r\n');
                for (let i = 0; i < files.length; i++) {
                    await uploadFile(files[i], files[i].name);
                }
            }
        }, false);

        // Download button
        document.getElementById('download-btn').addEventListener('click', () => {
            openFileBrowser('.');
        });

        // File browser path input
        document.getElementById('current-path-input').addEventListener('keydown', (e) => {
            if (e.key === 'Enter') {
                const newPath = e.target.value.trim();
                if (newPath) {
                    currentBrowserPath = newPath;
                    selectedFile = null;
                    document.getElementById('download-selected-btn').disabled = true;
                    loadDirectory(newPath);
                }
            }
        });

        // Download selected file
        document.getElementById('download-selected-btn').addEventListener('click', async () => {
            if (!selectedFile) return;
            await downloadFile(selectedFile.path, selectedFile.is_dir);
            closeFileBrowser();
        });
    }

    // Start initialization when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initialize);
    } else {
        initialize();
    }

    // Expose only safe functions for HTML onclick attributes
    window.closeFileBrowser = closeFileBrowser;
})();
