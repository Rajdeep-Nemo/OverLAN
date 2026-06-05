package main

import (
	"bufio"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

//go:embed index.html
var receiverHTML []byte

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
	w.Write(receiverHTML)
}

// Helper to broadcast JSON data to all connected SSE clients
func broadcastSSE(eventType string, data any) {
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

// Handles Uploads from the browser back to the Go server
func handleUpload(w http.ResponseWriter, r *http.Request) {
	// Limit memory usage to 32MB, the rest is streamed to disk
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Retrieve the file from the form data
	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Find the user's home directory across any OS (Windows/Mac/Linux)
	homeDir, err := os.UserHomeDir()
	if err != nil {
		http.Error(w, "Could not find home directory", http.StatusInternalServerError)
		return
	}

	downloadFolder := filepath.Join(homeDir, "Downloads")

	// Verify if the Downloads folder actually exists
	if _, err := os.Stat(downloadFolder); os.IsNotExist(err) {
		downloadFolder = homeDir
	}

	// Route the file
	savePath := filepath.Join(downloadFolder, header.Filename)
	dst, err := os.Create(savePath)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer dst.Close()

	// Copy the data to the local disk
	if _, err := io.Copy(dst, file); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Printf("\nReceived file from browser: %s\n", savePath)
	w.WriteHeader(http.StatusOK)
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
	mux.HandleFunc("/", handleHome)             // Serves the browser UI
	mux.HandleFunc("/events", handleSSE)        // SSE stream
	mux.HandleFunc("/accept", handleAccept)     // Accept handler
	mux.HandleFunc("/download", handleDownload) // Serves file
	mux.HandleFunc("/upload", handleUpload)     // Handles upload

	fileChan := make(chan string)

	// Runs path reading in a background goroutine
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			path := strings.TrimSpace(scanner.Text())
			path = strings.Trim(path, `"'`)
			if path != "" {
				fileChan <- path
			}
		}

		if err := scanner.Err(); err != nil {
			fmt.Fprintf(os.Stderr, "\nError reading input: %v\n", err)
		}
		close(fileChan)
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
