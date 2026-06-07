# OverLAN

A lightweight, zero-dependency file transfer tool for your local network. Run it on one machine, open the browser UI on another, and drag-and-drop files between them — no cloud, no accounts, no installation on the receiving end.

---

## How it works

OverLAN embeds the entire web UI (`index.html`) directly into the compiled binary, so there is nothing to deploy beyond the single executable. When running:

- The **host machine** drags files into the terminal (or types file paths) to queue them for sharing.
- Any device on the same network opens `http://<host-ip>:8080` in a browser to see the live queue.
- The **browser side** can accept (download) or reject queued files, and can also send files back to the host — which are saved automatically to the host's `~/Downloads` folder.
- A transfer history (last 50 entries) is kept for the session and shown in the History tab.

Real-time updates between the server and all open browser tabs are handled via **Server-Sent Events (SSE)** — no WebSockets or polling needed.

---

## Features

- Bidirectional transfer: send files *to* the browser, and receive files *from* the browser
- Live queue updates pushed instantly to all connected browser tabs
- Upload progress bar with cancel support
- Per-session transfer history (DOWNLOADED / UPLOADED / REJECTED)
- Single self-contained binary — the HTML UI is embedded at compile time
- Zero client-side installation required

---

## Requirements

- [Go](https://go.dev/dl/) 1.16 or newer

---

## Build

```bash
go build -o overlan main.go
```

This produces a single binary with the HTML UI baked in.

---

## Usage

1. Run the server on the machine that will be sharing files:

```bash
./overlan
```

You will see output like:
```
OverLAN Server Running!
Network Access: http://192.168.1.42:8080
--------------------------------------------------
Drag & drop files here and press ENTER to queue them...
```

2. On any other device on the same network, open the printed URL in a browser.

3. **To send a file to the browser:** drag a file onto the terminal window (most terminals will paste the path), then press `Enter`. The file will appear in the browser queue. The browser user can then click **Accept** to download it or **Reject** to dismiss it.

4. **To send a file from the browser to the host:** click **Send a File** in the browser UI, pick a file, and click **Send**. It will be saved to `~/Downloads` on the host machine.

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | Serves the embedded web UI |
| `GET` | `/events` | SSE stream for real-time queue and history updates |
| `GET` | `/download?id=<id>` | Download a queued file by ID |
| `GET` | `/reject?id=<id>` | Remove a file from the queue |
| `POST` | `/upload` | Receive a file uploaded from the browser |

---

## Project Structure

```
.
├── main.go       # Server logic, file queue, SSE broadcasting
└── index.html    # Browser UI (embedded into the binary at build time)
```

---

## Notes

- The server listens on port `8080`. Make sure this port is reachable on your local network (check your firewall if devices can't connect).
- Files received from the browser are saved to `~/Downloads`. If that folder doesn't exist, they fall back to the home directory.
- The in-memory queue and history are cleared when the server is restarted.
- OverLAN is intended for trusted local networks only. There is no authentication.