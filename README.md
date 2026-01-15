# tiny ttyd

## roadmap

- ttyd
- xterm.js
- auth
- session

## Tree


```
  src/
  ├── main.rs
  ├── server/
  │   ├── mod.rs
  │   └── listener.rs
  │
  ├── connection/
  │   ├── mod.rs
  │   ├── websocket.rs
  │   └── http.rs
  │
  ├── session/
  │   ├── mod.rs
  │   ├── manager.rs
  │   └── state.rs
  │
  ├── terminal/
  │   ├── mod.rs
  │   ├── pty.rs
  │   └── sync.rs
  │
  ├── fs/
  │   ├── mod.rs
  │   ├── upload.rs
  │   ├── download.rs
  │   ├── listing.rs
  │   └── archive.rs
  │
  ├── api/
  │   ├── mod.rs
  │   └── http_handlers.rs
  │
  ├── auth/
  │   ├── mod.rs
  │   ├── password_auth.rs
  │   └── none_auth.rs
  │
  └── config/
      ├── mod.rs
      └── loader.rs
```

