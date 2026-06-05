package main

import (
	"bufio"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
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

type HistoryItem struct {
	Time   string `json:"time"`
	Name   string `json:"name"`
	Status string `json:"status"`
}

var (
	mu    sync.Mutex
	queue = make(map[string]string)

	historyMu sync.Mutex
	history   []HistoryItem

	clientsMu sync.Mutex
	clients   = make(map[chan string]struct{})
)

func generateID() string {
	bytes := make([]byte, 4)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

func enqueueFile(path string) string {
	mu.Lock()
	defer mu.Unlock()
	id := generateID()
	queue[id] = path
	return id
}

func addHistory(name, status string) {
	historyMu.Lock()
	defer historyMu.Unlock()

	item := HistoryItem{
		Time:   time.Now().Format("15:04:05"),
		Name:   name,
		Status: status,
	}

	history = append([]HistoryItem{item}, history...)

	if len(history) > 50 {
		history = history[:50]
	}

	broadcastSSE("history_add", item)
}

func handleHome(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	w.Write(receiverHTML)
}

func handleSSE(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	clientChan := make(chan string)

	clientsMu.Lock()
	clients[clientChan] = struct{}{}
	clientsMu.Unlock()

	defer func() {
		clientsMu.Lock()
		delete(clients, clientChan)
		clientsMu.Unlock()
		close(clientChan)
	}()

	mu.Lock()
	currentQueue := make(map[string]string)
	for k, v := range queue {
		currentQueue[k] = v
	}
	mu.Unlock()

	for id, path := range currentQueue {
		info, err := os.Stat(path)
		if err == nil {
			data := map[string]any{"id": id, "name": filepath.Base(path), "size": info.Size()}
			jsonData, _ := json.Marshal(data)
			fmt.Fprintf(w, "event: file\ndata: %s\n\n", jsonData)
		}
	}

	historyMu.Lock()
	currentHistory := make([]HistoryItem, len(history))
	copy(currentHistory, history)
	historyMu.Unlock()

	if len(currentHistory) > 0 {
		jsonData, _ := json.Marshal(currentHistory)
		fmt.Fprintf(w, "event: history_sync\ndata: %s\n\n", jsonData)
	}

	flusher.Flush()

	for {
		select {
		case msg := <-clientChan:
			fmt.Fprint(w, msg)
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

func broadcastSSE(eventType string, data any) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return
	}
	msg := fmt.Sprintf("event: %s\ndata: %s\n\n", eventType, jsonData)

	clientsMu.Lock()
	defer clientsMu.Unlock()
	for clientChan := range clients {
		select {
		case clientChan <- msg:
		default:
		}
	}
}

func notifySSEClients(id string, filePath string) {
	info, err := os.Stat(filePath)
	if err != nil {
		fmt.Printf("[Error] Cannot read file (skipping): %s\n", err)
		return
	}
	broadcastSSE("file", map[string]any{
		"id":   id,
		"name": filepath.Base(filePath),
		"size": info.Size(),
	})
	fmt.Printf("Queued: %s\n", filepath.Base(filePath))
}

func handleReject(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	mu.Lock()
	path, exists := queue[id]
	delete(queue, id)
	mu.Unlock()

	if exists {
		addHistory(filepath.Base(path), "REJECTED")
	}

	w.WriteHeader(http.StatusOK)
}

func handleDownload(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")

	mu.Lock()
	path, exists := queue[id]
	mu.Unlock()

	if !exists {
		http.Error(w, "File not found or expired", http.StatusNotFound)
		return
	}

	info, err := os.Stat(path)
	if err != nil {
		http.Error(w, "Unable to read local file", http.StatusInternalServerError)
		return
	}

	addHistory(filepath.Base(path), "DOWNLOADED")

	w.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", "attachment; filename=\""+filepath.Base(path)+"\"")

	http.ServeFile(w, r, path)
}

// HIGH-PERFORMANCE STREAMING UPLOAD
func handleUpload(w http.ResponseWriter, r *http.Request) {
	// 1. Grab the raw network stream directly (NO Temp files, NO memory limits)
	reader, err := r.MultipartReader()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		http.Error(w, "Could not find home directory", http.StatusInternalServerError)
		return
	}

	downloadFolder := filepath.Join(homeDir, "Downloads")
	if _, err := os.Stat(downloadFolder); os.IsNotExist(err) {
		downloadFolder = homeDir
	}

	// 2. Iterate through the incoming form parts until we find the actual file
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break // Finished reading request
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// 3. If we found the file part, start streaming it to disk
		if part.FileName() != "" {
			savePath := filepath.Join(downloadFolder, part.FileName())
			dst, err := os.Create(savePath)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			// io.Copy writes from the network stream directly to the file stream.
			if _, err := io.Copy(dst, part); err != nil {
				// If the user clicks Cancel, io.Copy fails. We must close and delete the broken file.
				dst.Close()
				os.Remove(savePath)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			// Upload succeeded!
			dst.Close()

			addHistory(part.FileName(), "UPLOADED")
			fmt.Printf("\nReceived file from browser: %s\n", savePath)
			w.WriteHeader(http.StatusOK)
			return // We process one file per request, then exit
		}
	}

	http.Error(w, "No file found in request", http.StatusBadRequest)
}

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
	mux := http.NewServeMux()
	mux.HandleFunc("/", handleHome)
	mux.HandleFunc("/events", handleSSE)
	mux.HandleFunc("/reject", handleReject)
	mux.HandleFunc("/download", handleDownload)
	mux.HandleFunc("/upload", handleUpload)

	port := ":8080"
	fmt.Printf("OverLAN Server Running!\n")
	fmt.Printf("Network Access: http://%s%s\n", getLocalIP(), port)
	fmt.Println("--------------------------------------------------")
	fmt.Println("Drag & drop files here and press ENTER to queue them...")

	fileChan := make(chan string)

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

	go func() {
		if err := http.ListenAndServe(port, mux); err != nil {
			fmt.Printf("Server crashed: %v\n", err)
		}
	}()

	for path := range fileChan {
		id := enqueueFile(path)
		notifySSEClients(id, path)
	}
}
