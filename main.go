package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// HTML Page on receiver's end
const receiverHTML = `<!DOCTYPE html>
<html>
<head>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>OverLAN</title>
  <style>
      :root {
        --primary: #3b9e62;
        --bg: #f5f5f3;
        --card-bg: #ffffff;
      }

      * { box-sizing: border-box; margin: 0; padding: 0; }
      body {
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
        background: var(--bg);
        min-height: 100vh;
        display: flex; flex-direction: column; align-items: center;
      }

      /* Top Island */
      .topbar {
        margin-top: 20px;
        background: var(--card-bg);
        padding: 10px 20px;
        border-radius: 50px;
        border: 1px solid rgba(0,0,0,0.05);
        box-shadow: 0 4px 12px rgba(0,0,0,0.05);
        display: flex; align-items: center; gap: 12px;
        z-index: 10;
      }
      .topbar h1 { font-size: 14px; font-weight: 600; text-transform: uppercase; letter-spacing: 1px; color: #555; }
      .dot { width: 8px; height: 8px; border-radius: 50%; background: #dee2e6; transition: background .3s; }
      .dot.live { background: var(--primary); box-shadow: 0 0 8px var(--primary); }

      /* Main Content Island */
      .body {
        flex: 1;
        display: flex; align-items: center; justify-content: center;
        width: 100%;
        padding: 24px;
      }

      .card {
        background: var(--card-bg);
        border-radius: 20px;
        padding: 24px;
        width: 100%; max-width: 380px;
        box-shadow: 0 10px 30px rgba(0,0,0,0.08);
        border: 1px solid rgba(0,0,0,0.03);
      }

      .card-header { margin-bottom: 24px; }
      .file-name { font-size: 17px; font-weight: 600; word-break: break-all; margin-bottom: 4px; }
      .file-size { font-size: 14px; color: #888; }

      .actions { display: flex; gap: 12px; }
      button { flex: 1; padding: 12px; border-radius: 10px; font-weight: 600; cursor: pointer; border: none; background: #f0f0f0; transition: .2s; }
      button:hover { background: #e5e5e5; }
      .btn-accept { background: var(--primary); color: white; }
      .btn-accept:hover { background: #328653; }

      .progress-track { height: 6px; border-radius: 3px; background: #eee; overflow: hidden; margin: 16px 0; }
      .progress-fill { height: 100%; background: var(--primary); width: 0%; transition: width .3s; }
      .progress-meta { display: flex; justify-content: space-between; font-size: 13px; color: #888; font-weight: 500; }
    </style>
  </head>
  <body>
  <div class="topbar">
      <h1>Share</h1>
      <div class="dot" id="dot"></div>
    </div>

    <div class="body" id="body">
      <div class="empty">
        <p style="color: #aaa;">Waiting for incoming files…</p>
      </div>
    </div>

  <script>
    const es = new EventSource('/events')

    es.addEventListener('file', e => {
      const f = JSON.parse(e.data)
      document.getElementById('dot').classList.add('live')
      document.getElementById('body').innerHTML = `
              <div class="card">
                <div class="card-header">
                	<div class="file-name">${f.name}</div>
                  <div class="file-size">${formatSize(f.size)}</div>
                </div>
                <div class="actions">
                  <button onclick="reject()">Reject</button>
                  <button class="btn-accept" onclick="accept()">Accept</button>
                </div>
              </div>`
    })

    function accept() {
      fetch('/accept', { method: 'POST' })
      showProgress()
    }

    function reject() {
      fetch('/reject', { method: 'POST' })
      document.getElementById('dot').classList.remove('live')
      document.getElementById('body').innerHTML = '<div class="empty"><p>Waiting for incoming files…</p></div>'
    }

    function showProgress() {
      document.getElementById('body').innerHTML = ` + "`" + `
        <div class="card">
          <div class="progress-meta"><span id="pct">0%</span><span id="spd"></span></div>
          <div class="progress-track"><div class="progress-fill" id="bar"></div></div>
          <div style="font-size:13px;color:#888;margin-top:8px" id="status">Transferring…</div>
        </div>` + "`" + `

      // Trigger the actual download — browser saves the file
      const a = document.createElement('a')
      a.href = '/download'
      a.download = ''
      document.body.appendChild(a)
      a.click()
      document.body.removeChild(a)
    }

    // SSE progress events from the server during transfer
    es.addEventListener('progress', e => {
      const p = JSON.parse(e.data)
      const pct = Math.round((p.sent / p.total) * 100)
      document.getElementById('bar').style.width = pct + '%'
      document.getElementById('pct').textContent = pct + '%'
      document.getElementById('spd').textContent = formatSize(p.speed) + '/s'
      if (pct >= 100) {
        document.getElementById('status').textContent = 'Saved to downloads'
        document.getElementById('dot').classList.remove('live')
      }
    })

    function formatSize(bytes) {
      if (bytes < 1024) return bytes + ' B'
      if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB'
      return (bytes / 1048576).toFixed(1) + ' MB'
    }
  </script>
</body>
</html>`

// Global state protected by a mutex
var (
	mu          sync.Mutex
	pendingFile string
	fileReady   bool

	clientsMu sync.Mutex
	clients   = make(map[chan string]struct{})
)

// Set the pending file path
func setPendingFile(path string) {
	mu.Lock()
	pendingFile = path
	fileReady = true
	mu.Unlock()
}

// Get the file states
func getPendingFile() (string, bool) {
	mu.Lock()
	defer mu.Unlock()
	return pendingFile, fileReady
}

// Handles Home route
func handleHome(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	fmt.Fprint(w, receiverHTML)
}

// Helper to broadcast JSON data to all connected SSE clients
func broadcastSSE(eventType string, data interface{}) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return
	}
	msg := fmt.Sprintf("event: %s\ndata: %s\n\n", eventType, jsonData)

	clientsMu.Lock()
	defer clientsMu.Unlock()
	for clientChan := range clients {
		// Non-blocking send
		select {
		case clientChan <- msg:
		default:
		}
	}
}

// Notifies clients that a new file is ready
func notifySSEClients(filePath string) {
	info, err := os.Stat(filePath)
	if err != nil {
		fmt.Printf("Error reading file stat: %v\n", err)
		return
	}

	broadcastSSE("file", map[string]interface{}{
		"name": filepath.Base(filePath),
		"size": info.Size(),
	})
}

// Handles SSE Connections
func handleSSE(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	// Create a channel for this specific client
	clientChan := make(chan string)

	// Register client
	clientsMu.Lock()
	clients[clientChan] = struct{}{}
	clientsMu.Unlock()

	// De-register client when they disconnect
	defer func() {
		clientsMu.Lock()
		delete(clients, clientChan)
		clientsMu.Unlock()
		close(clientChan)
	}()

	// If a file is already queued when they connect, notify them immediately
	if path, ready := getPendingFile(); ready {
		notifySSEClients(path)
	}

	// Listen for messages to send to this client
	for {
		select {
		case msg := <-clientChan:
			fmt.Fprint(w, msg)
			flusher.Flush()
		case <-r.Context().Done():
			return // Client disconnected
		}
	}
}

// Handles Downloads
func handleDownload(w http.ResponseWriter, r *http.Request) {
	path, _ := getPendingFile()
	file, _ := os.Open(path)
	defer file.Close()

	info, _ := file.Stat()
	total := info.Size()

	w.Header().Set("Content-Disposition", "attachment; filename=\""+filepath.Base(path)+"\"")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(total, 10))

	// Copy in chunks and broadcast progress via SSE
	buf := make([]byte, 32*1024) // 32 KB chunks
	var sent int64
	start := time.Now()

	for {
		n, err := file.Read(buf)
		if n > 0 {
			w.Write(buf[:n])
			sent += int64(n)
			elapsed := time.Since(start).Seconds()
			speed := int64(float64(sent) / elapsed)

			// Notify all SSE clients about progress
			broadcastSSE("progress", map[string]int64{
				"sent":  sent,
				"total": total,
				"speed": speed,
			})
		}
		if err != nil {
			break
		}
	}
}

// Handlers for accept/reject buttons
func handleAccept(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }
func handleReject(w http.ResponseWriter)                  { w.WriteHeader(http.StatusOK) }

// Returns local ip
func getLocalIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "localhost"
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String()
}

func main() {
	fmt.Printf("Receiver should open: http://%s:8080\n", getLocalIP())
	fmt.Println("Type a file path and press ENTER to share it...")
	// Setting up the routes
	mux := http.NewServeMux()
	mux.HandleFunc("/", handleHome)             // serves the browser UI
	mux.HandleFunc("/events", handleSSE)        // SSE stream
	mux.HandleFunc("/accept", handleAccept)     // receiver clicks Accept
	mux.HandleFunc("/download", handleDownload) // actual file bytes

	fileChan := make(chan string)

	// Run path reading in a background goroutine
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			path := strings.TrimSpace(scanner.Text())
			path = strings.Trim(path, `"'`)
			if path != "" {
				fileChan <- path
			}
		}
	}()
	// Runs the HTTP server in a background goroutine
	go func() {
		if err := http.ListenAndServe(":8080", mux); err != nil {
			fmt.Printf("HTTP server error: %v\n", err)
		}
	}()
	// Main goroutine
	for path := range fileChan {
		setPendingFile(path)
		notifySSEClients(path)
		fmt.Printf("Shared: %s\n", path)
	}
}
