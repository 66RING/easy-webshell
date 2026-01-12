#!/usr/bin/env python3
"""
Simple TTYD implementation - Share terminal over web
Uses WebSocket to bridge browser and pseudo-terminal
"""

import asyncio
import websockets
from websockets.asyncio.server import Response
from websockets.datastructures import Headers
import pty
import os
import tty
import termios
import struct
import fcntl
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class PTYSession:
    """Manages a pseudo-terminal session"""

    def __init__(self, cols=80, rows=24):
        self.pid, self.fd = pty.fork()
        self.cols = cols
        self.rows = rows

        if self.pid == 0:
            # Child process - spawn shell
            os.execvp(os.environ.get('SHELL', 'bash'), ['bash'])
        else:
            # Parent process - set terminal size and make PTY non-blocking
            self.set_winsize(cols, rows)
            # Make PTY non-blocking so reads don't block
            import os as os_module
            os_module.set_blocking(self.fd, False)

    def set_winsize(self, cols, rows):
        """Set terminal window size"""
        if self.fd is None:
            return

        # Construct terminal size structure
        winsize = struct.pack('HHHH', rows, cols, 0, 0)
        fcntl.ioctl(self.fd, termios.TIOCSWINSZ, winsize)

    def read(self, size=1024):
        """Read output from pseudo-terminal"""
        try:
            return os.read(self.fd, size)
        except OSError:
            return b''

    def write(self, data):
        """Write input to pseudo-terminal"""
        try:
            os.write(self.fd, data)
        except OSError:
            pass

    def close(self):
        """Close the pseudo-terminal"""
        if self.fd is not None:
            os.close(self.fd)
        if self.pid is not None:
            try:
                os.kill(self.pid, 9)
                os.waitpid(self.pid, 0)
            except ChildProcessError:
                pass


async def handle_websocket(websocket):
    """Handle WebSocket connection from browser"""

    # Get the path from the connection
    path = websocket.request.path

    # Only accept connections on /ws path
    if path != '/ws':
        logger.info(f"Rejected connection on path: {path}")
        await websocket.close(1008, "Invalid path")
        return

    logger.info(f"New connection from {websocket.remote_address}")

    # Create new PTY session
    pty_session = PTYSession()

    async def forward_pty_to_ws():
        """Forward PTY output to WebSocket client"""
        try:
            loop = asyncio.get_event_loop()
            while True:
                # Read from PTY in non-blocking way
                data = await loop.run_in_executor(None, pty_session.read, 4096)
                if data:
                    logger.debug(f"PTY -> WS: {len(data)} bytes")
                    try:
                        # Decode bytes to string for xterm.js (expects TEXT frames, not BINARY)
                        await websocket.send(data.decode('utf-8', errors='ignore'))
                    except websockets.exceptions.ConnectionClosed:
                        raise
                else:
                    await asyncio.sleep(0.01)
        except websockets.exceptions.ConnectionClosed:
            logger.info("PTY to WS forwarder stopped")
        except Exception as e:
            logger.error(f"Error forwarding PTY to WS: {e}")

    async def forward_ws_to_pty():
        """Forward WebSocket input to PTY"""
        try:
            async for message in websocket:
                # In websockets 15+, messages are strings, need to encode for PTY
                # Check if it's a resize message (JSON format)
                if isinstance(message, str) and message.startswith('{'):
                    try:
                        import json
                        data = json.loads(message)
                        if 'cols' in data and 'rows' in data:
                            pty_session.set_winsize(data['cols'], data['rows'])
                            logger.info(f"Terminal resized to {data['cols']}x{data['rows']}")
                    except json.JSONDecodeError:
                        # Not JSON, treat as regular input
                        if isinstance(message, str):
                            logger.debug(f"WS -> PTY: {repr(message[:20])}")
                            pty_session.write(message.encode())
                        else:
                            logger.debug(f"WS -> PTY: {len(message)} bytes")
                            pty_session.write(message)
                else:
                    # Regular input to PTY
                    if isinstance(message, str):
                        logger.debug(f"WS -> PTY: {repr(message[:20])}")
                        pty_session.write(message.encode())
                    else:
                        logger.debug(f"WS -> PTY: {len(message)} bytes")
                        pty_session.write(message)
        except websockets.exceptions.ConnectionClosed:
            logger.info("WS to PTY forwarder stopped")
        except Exception as e:
            logger.error(f"Error forwarding WS to PTY: {e}")

    # Run both directions concurrently
    try:
        await asyncio.gather(
            forward_pty_to_ws(),
            forward_ws_to_pty()
        )
    except websockets.exceptions.ConnectionClosed:
        logger.info("WebSocket connection closed")
    finally:
        pty_session.close()
        logger.info("PTY session closed")


def get_html_content():
    """Generate HTML page for terminal interface"""
    return '''
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>TTYD - Simple Terminal</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/xterm@5.3.0/css/xterm.css" />
    <script src="https://cdn.jsdelivr.net/npm/xterm@5.3.0/lib/xterm.js"></script>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        body {
            height: 100vh;
            background: #1e1e1e;
        }
        #terminal {
            width: 100%;
            height: 100%;
        }
    </style>
</head>
<body>
    <div id="terminal"></div>
    <script>
        // Create xterm.js instance
        const term = new Terminal({
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

        // Connect to WebSocket server
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const ws = new WebSocket(`${protocol}//${window.location.host}/ws`);

        ws.onopen = () => {
            console.log('WebSocket connected');

            // Send terminal size
            const resize = {
                cols: term.cols,
                rows: term.rows
            };
            ws.send(JSON.stringify(resize));
        };

        ws.onmessage = (event) => {
            // Write data from PTY to terminal
            term.write(event.data);
        };

        ws.onclose = () => {
            console.log('WebSocket disconnected');
            term.write('\\r\\n\\x1b[31mConnection closed\\x1b[0m');
        };

        ws.onerror = (error) => {
            console.error('WebSocket error:', error);
        };

        // Send user input to PTY
        term.onData(data => {
            ws.send(data);
        });

        // Handle terminal resize
        term.onResize(size => {
            const resize = {
                cols: size.cols,
                rows: size.rows
            };
            ws.send(JSON.stringify(resize));
        });

        // Handle window resize
        window.addEventListener('resize', () => {
            term.fit();
        });
    </script>
</body>
</html>
    '''


async def handle_http_request(connection, request):
    """Handle HTTP requests and serve HTML page"""
    # Log incoming request for debugging
    logger.info(f"HTTP request: path={request.path}")
    logger.info(f"Upgrade header: {request.headers.get('Upgrade', 'none')}")

    # Check if this is a WebSocket upgrade request
    upgrade_header = request.headers.get('Upgrade', '')
    if 'websocket' in upgrade_header.lower():
        # Let websockets handle the WebSocket handshake
        return None

    # Handle regular HTTP requests
    if request.path == '/':
        html = get_html_content()
        headers = Headers()
        headers['Content-Type'] = 'text/html'
        return Response(
            status_code=200,
            reason_phrase='OK',
            headers=headers,
            body=html.encode()
        )
    else:
        headers = Headers()
        headers['Content-Type'] = 'text/plain'
        return Response(
            status_code=404,
            reason_phrase='Not Found',
            headers=headers,
            body=b'Not Found'
        )


async def main():
    """Main entry point - start WebSocket and HTTP server"""

    host = '0.0.0.0'
    port = 7681

    logger.info(f"Starting TTYD server on {host}:{port}")
    logger.info(f"Open http://localhost:{port} in your browser")

    # Create WebSocket server with HTTP handler
    async with websockets.serve(
        handle_websocket,
        host,
        port,
        process_request=handle_http_request
    ):
        await asyncio.Future()  # Run forever


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
