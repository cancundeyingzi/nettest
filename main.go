package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/http/httptrace"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
)

// ==================== 数据结构 ====================

type Target struct {
	ID      string `json:"id"`
	URL     string `json:"url"`
	Name    string `json:"name"`
	Enabled bool   `json:"enabled"`
}

type TestResult struct {
	Timestamp  int64   `json:"timestamp"`
	Latency    float64 `json:"latency"`
	Speed      float64 `json:"speed"`
	PacketLoss float64 `json:"packetLoss"`
	BytesRecv  int64   `json:"bytesRecv"`
	BytesSent  int64   `json:"bytesSent"`
	Success    bool    `json:"success"`
	Error      string  `json:"error,omitempty"`
}

type Stats struct {
	CurrentSpeed     float64 `json:"currentSpeed"`
	AvgLatency       float64 `json:"avgLatency"`
	PacketLoss       float64 `json:"packetLoss"`
	PacketLossRecent float64 `json:"packetLossRecent"`
	TotalBytesRecv   int64   `json:"totalBytesRecv"`
	TotalBytesSent   int64   `json:"totalBytesSent"`
	TestCount        int64   `json:"testCount"`
	SuccessCount     int64   `json:"successCount"`
	ActiveThreads    int     `json:"activeThreads"`
	PerMinute        float64 `json:"perMinute"`
	PerHour          float64 `json:"perHour"`
	PerDay           float64 `json:"perDay"`
	PerMonth         float64 `json:"perMonth"`
}

type Config struct {
	Threads  int  `json:"threads"`
	Interval int  `json:"interval"`
	Running  bool `json:"running"`
}

// ==================== 网络测试引擎 ====================

type SpeedSample struct {
	Timestamp int64
	Bytes     int64
	Success   bool
}

type ThroughputSample struct {
	Timestamp  int64
	TotalBytes int64
}

type countingWriter struct {
	counter *atomic.Int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n := len(p)
	w.counter.Add(int64(n))
	return n, nil
}

type NetworkTester struct {
	mu                sync.RWMutex
	targets           map[string]*Target
	config            Config
	stats             Stats
	results           []TestResult
	maxResults        int
	running           atomic.Bool
	activeThreads     atomic.Int32
	cancelFunc        context.CancelFunc
	clients           map[*websocket.Conn]bool
	clientsMu         sync.RWMutex
	totalRecv         atomic.Int64
	totalSent         atomic.Int64
	testCount         atomic.Int64
	successCount      atomic.Int64
	latencySum        atomic.Int64
	latencyCount      atomic.Int64
	speedSamples      []SpeedSample
	throughputSamples []ThroughputSample
	speedMu           sync.Mutex
	lastSpeedBits     atomic.Uint64
	httpClient        *http.Client
	gcCounter         int
}

func NewNetworkTester() *NetworkTester {
	nt := &NetworkTester{
		targets:    make(map[string]*Target),
		config:     Config{Threads: 4, Interval: 1000, Running: false},
		results:    make([]TestResult, 0, 1000),
		maxResults: 1000,
		clients:    make(map[*websocket.Conn]bool),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout:   5 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				MaxIdleConns:          100,
				MaxIdleConnsPerHost:   10,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   5 * time.Second,
				ResponseHeaderTimeout: 5 * time.Second,
				DisableCompression:    true,
				ForceAttemptHTTP2:     false,
			},
		},
	}
	nt.loadTargets()
	return nt
}

func (nt *NetworkTester) loadTargets() {
	data, err := os.ReadFile("targets.json")
	if err != nil {
		return
	}
	var targets []*Target
	if json.Unmarshal(data, &targets) == nil {
		for _, t := range targets {
			nt.targets[t.ID] = t
		}
	}
}

func (nt *NetworkTester) saveTargets() {
	nt.mu.RLock()
	targets := make([]*Target, 0, len(nt.targets))
	for _, t := range nt.targets {
		targets = append(targets, t)
	}
	nt.mu.RUnlock()
	data, _ := json.MarshalIndent(targets, "", "  ")
	os.WriteFile("targets.json", data, 0644)
}

func (nt *NetworkTester) AddTarget(t *Target) {
	nt.mu.Lock()
	nt.targets[t.ID] = t
	nt.mu.Unlock()
	nt.saveTargets()
}

func (nt *NetworkTester) RemoveTarget(id string) {
	nt.mu.Lock()
	delete(nt.targets, id)
	nt.mu.Unlock()
	nt.saveTargets()
}

func (nt *NetworkTester) GetTargets() []*Target {
	nt.mu.RLock()
	defer nt.mu.RUnlock()
	targets := make([]*Target, 0, len(nt.targets))
	for _, t := range nt.targets {
		targets = append(targets, t)
	}
	return targets
}

func (nt *NetworkTester) SetConfig(c Config) {
	nt.mu.Lock()
	wasRunning := nt.config.Running
	nt.config = c
	nt.mu.Unlock()

	if c.Running && !wasRunning {
		nt.Start()
	} else if !c.Running && wasRunning {
		nt.Stop()
	}
}

func (nt *NetworkTester) GetConfig() Config {
	nt.mu.RLock()
	defer nt.mu.RUnlock()
	c := nt.config
	c.Running = nt.running.Load()
	return c
}

func (nt *NetworkTester) Start() {
	if nt.running.Swap(true) {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	nt.cancelFunc = cancel

	nt.mu.RLock()
	threads := nt.config.Threads
	nt.mu.RUnlock()

	for i := 0; i < threads; i++ {
		go nt.worker(ctx)
	}

	go nt.statsUpdater(ctx)
}

func (nt *NetworkTester) Stop() {
	if !nt.running.Swap(false) {
		return
	}
	if nt.cancelFunc != nil {
		nt.cancelFunc()
	}
}

func (nt *NetworkTester) worker(ctx context.Context) {
	nt.activeThreads.Add(1)
	defer nt.activeThreads.Add(-1)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			nt.runTest(ctx)
		}
	}
}

func (nt *NetworkTester) runTest(ctx context.Context) {
	nt.mu.RLock()
	var target *Target
	for _, t := range nt.targets {
		if t.Enabled {
			target = t
			break
		}
	}
	nt.mu.RUnlock()

	if target == nil {
		return
	}

	testCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	result := TestResult{
		Timestamp: time.Now().UnixMilli(),
	}

	start := time.Now()

	var firstByteTime time.Time

	trace := &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
		},
		GotFirstResponseByte: func() {
			if firstByteTime.IsZero() {
				firstByteTime = time.Now()
			}
		},
	}

	req, err := http.NewRequestWithContext(testCtx, "GET", target.URL, nil)
	if err != nil {
		result.Error = err.Error()
		nt.recordResult(result)
		return
	}
	req.Header.Set("Accept-Encoding", "identity")
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

	resp, err := nt.httpClient.Do(req)
	if err != nil {
		result.Error = err.Error()
		nt.testCount.Add(1)
		nt.recordFailedSample()
		nt.recordResult(result)
		return
	}
	defer resp.Body.Close()

	// ==================== 计算 HTTP 字节（不估算 TCP/TLS 开销） ====================
	// 请求行: "GET /path HTTP/1.1\r\n"
	reqLine := fmt.Sprintf("GET %s HTTP/1.1\r\n", req.URL.RequestURI())
	reqHeaderSize := len(reqLine)
	reqHeaderSize += len(fmt.Sprintf("Host: %s\r\n", req.URL.Host))
	for key, values := range req.Header {
		for _, value := range values {
			reqHeaderSize += len(fmt.Sprintf("%s: %s\r\n", key, value))
		}
	}
	if req.Header.Get("User-Agent") == "" {
		reqHeaderSize += len("User-Agent: Go-http-client/1.1\r\n")
	}
	if req.Header.Get("Accept-Encoding") == "" {
		reqHeaderSize += len("Accept-Encoding: gzip\r\n")
	}
	reqHeaderSize += 2

	respHeaderSize := len(fmt.Sprintf("HTTP/1.1 %d %s\r\n", resp.StatusCode, http.StatusText(resp.StatusCode)))
	for key, values := range resp.Header {
		for _, value := range values {
			respHeaderSize += len(fmt.Sprintf("%s: %s\r\n", key, value))
		}
	}
	respHeaderSize += 2

	nt.totalSent.Add(int64(reqHeaderSize))
	nt.totalRecv.Add(int64(respHeaderSize))

	transferStart := time.Now()
	payloadBytes, err := io.Copy(&countingWriter{counter: &nt.totalRecv}, resp.Body)
	transferElapsed := time.Since(transferStart)
	elapsed := time.Since(start)

	result.BytesSent = int64(reqHeaderSize)
	result.BytesRecv = int64(respHeaderSize) + payloadBytes
	var latencyMs float64
	if !firstByteTime.IsZero() {
		latencyMs = float64(firstByteTime.Sub(start).Milliseconds())
	} else {
		latencyMs = float64(elapsed.Milliseconds())
	}
	result.Latency = latencyMs

	if err != nil {
		result.Error = err.Error()
		nt.testCount.Add(1)
		nt.recordFailedSample()
		nt.recordResult(result)
		return
	}

	// 使用纯数据传输时间计算网速
	if transferElapsed.Seconds() > 0 {
		result.Speed = float64(result.BytesRecv) / transferElapsed.Seconds()
	} else {
		result.Speed = float64(result.BytesRecv) / elapsed.Seconds()
	}
	result.Success = true

	nt.testCount.Add(1)
	nt.successCount.Add(1)
	nt.latencySum.Add(int64(result.Latency * 1000))
	nt.latencyCount.Add(1)

	now := time.Now().UnixMilli()
	nt.speedMu.Lock()
	nt.speedSamples = append(nt.speedSamples, SpeedSample{
		Timestamp: now,
		Bytes:     result.BytesRecv,
		Success:   true,
	})
	cutoff := now - 10000
	newStart := 0
	for i, s := range nt.speedSamples {
		if s.Timestamp >= cutoff {
			newStart = i
			break
		}
	}
	if newStart > 0 {
		nt.speedSamples = nt.speedSamples[newStart:]
	}
	nt.speedMu.Unlock()

	nt.recordResult(result)
}

func (nt *NetworkTester) recordFailedSample() {
	now := time.Now().UnixMilli()
	nt.speedMu.Lock()
	nt.speedSamples = append(nt.speedSamples, SpeedSample{
		Timestamp: now,
		Bytes:     0,
		Success:   false,
	})
	cutoff := now - 10000
	newStart := 0
	for i, s := range nt.speedSamples {
		if s.Timestamp >= cutoff {
			newStart = i
			break
		}
	}
	if newStart > 0 {
		nt.speedSamples = nt.speedSamples[newStart:]
	}
	nt.speedMu.Unlock()
}

func (nt *NetworkTester) recordResult(result TestResult) {
	nt.mu.Lock()
	nt.results = append(nt.results, result)
	if len(nt.results) > nt.maxResults {
		nt.results = nt.results[len(nt.results)-nt.maxResults:]
	}
	nt.mu.Unlock()
}

func (nt *NetworkTester) statsUpdater(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats := nt.calculateStats()
			nt.broadcast(stats)
			nt.gcCounter++
			if nt.gcCounter >= 60 {
				runtime.GC()
				nt.gcCounter = 0
			}
		}
	}
}

func (nt *NetworkTester) calculateStats() Stats {
	testCount := nt.testCount.Load()
	successCount := nt.successCount.Load()

	var avgLatency float64
	if count := nt.latencyCount.Load(); count > 0 {
		avgLatency = float64(nt.latencySum.Load()) / float64(count) / 1000.0
	}

	var packetLoss float64
	if testCount > 0 {
		packetLoss = float64(testCount-successCount) / float64(testCount) * 100
	}

	now := time.Now().UnixMilli()
	totalBytes := nt.totalRecv.Load() + nt.totalSent.Load()
	nt.speedMu.Lock()
	var recentTotal, recentSuccess int

	for _, s := range nt.speedSamples {
		recentTotal++
		if s.Success {
			recentSuccess++
		}
	}

	const throughputWindowMs int64 = 1000
	const throughputHistoryMs int64 = 10000
	nt.throughputSamples = append(nt.throughputSamples, ThroughputSample{
		Timestamp:  now,
		TotalBytes: totalBytes,
	})
	cutoffWindow := now - throughputHistoryMs
	throughputStart := 0
	for i, s := range nt.throughputSamples {
		if s.Timestamp >= cutoffWindow {
			throughputStart = i
			break
		}
	}
	if throughputStart > 0 {
		nt.throughputSamples = nt.throughputSamples[throughputStart:]
	}

	var currentSpeed float64
	if len(nt.throughputSamples) >= 2 {
		newest := nt.throughputSamples[len(nt.throughputSamples)-1]
		targetOldestTs := newest.Timestamp - throughputWindowMs
		oldest := nt.throughputSamples[0]
		for i := len(nt.throughputSamples) - 1; i >= 0; i-- {
			if nt.throughputSamples[i].Timestamp <= targetOldestTs {
				oldest = nt.throughputSamples[i]
				break
			}
		}
		elapsedMs := newest.Timestamp - oldest.Timestamp
		if elapsedMs > 0 && newest.TotalBytes >= oldest.TotalBytes {
			currentSpeed = float64(newest.TotalBytes-oldest.TotalBytes) / (float64(elapsedMs) / 1000.0)
		}
	}
	nt.speedMu.Unlock()

	prevSpeed := math.Float64frombits(nt.lastSpeedBits.Load())
	if currentSpeed > 0 {
		nt.lastSpeedBits.Store(math.Float64bits(currentSpeed))
	} else if prevSpeed > 0 {
		currentSpeed = prevSpeed
	}

	var packetLossRecent float64
	if recentTotal > 0 {
		packetLossRecent = float64(recentTotal-recentSuccess) / float64(recentTotal) * 100
	}

	return Stats{
		CurrentSpeed:     currentSpeed,
		AvgLatency:       avgLatency,
		PacketLoss:       packetLoss,
		PacketLossRecent: packetLossRecent,
		TotalBytesRecv:   totalBytes - nt.totalSent.Load(),
		TotalBytesSent:   nt.totalSent.Load(),
		TestCount:        testCount,
		SuccessCount:     successCount,
		ActiveThreads:    int(nt.activeThreads.Load()),
		PerMinute:        currentSpeed * 60,
		PerHour:          currentSpeed * 3600,
		PerDay:           currentSpeed * 86400,
		PerMonth:         currentSpeed * 86400 * 30,
	}
}

func (nt *NetworkTester) GetResults() []TestResult {
	nt.mu.RLock()
	defer nt.mu.RUnlock()
	results := make([]TestResult, len(nt.results))
	copy(results, nt.results)
	return results
}

func (nt *NetworkTester) broadcast(stats Stats) {
	data, _ := json.Marshal(map[string]interface{}{
		"type":  "stats",
		"stats": stats,
	})

	nt.clientsMu.RLock()
	defer nt.clientsMu.RUnlock()

	for client := range nt.clients {
		client.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if err := client.WriteMessage(websocket.TextMessage, data); err != nil {
			go nt.removeClient(client)
		}
	}
}

func (nt *NetworkTester) addClient(conn *websocket.Conn) {
	nt.clientsMu.Lock()
	nt.clients[conn] = true
	nt.clientsMu.Unlock()
}

func (nt *NetworkTester) removeClient(conn *websocket.Conn) {
	nt.clientsMu.Lock()
	delete(nt.clients, conn)
	nt.clientsMu.Unlock()
	conn.Close()
}

// ==================== HTTP处理器 ====================

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func (nt *NetworkTester) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	nt.addClient(conn)

	stats := nt.calculateStats()
	data, _ := json.Marshal(map[string]interface{}{
		"type":  "stats",
		"stats": stats,
	})
	conn.WriteMessage(websocket.TextMessage, data)

	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			nt.removeClient(conn)
			return
		}
	}
}

func (nt *NetworkTester) handleTargets(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case "GET":
		json.NewEncoder(w).Encode(nt.GetTargets())
	case "POST":
		var target Target
		if err := json.NewDecoder(r.Body).Decode(&target); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if target.ID == "" {
			target.ID = fmt.Sprintf("%d", time.Now().UnixNano())
		}
		nt.AddTarget(&target)
		json.NewEncoder(w).Encode(target)
	}
}

func (nt *NetworkTester) handleTarget(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	vars := mux.Vars(r)
	id := vars["id"]

	switch r.Method {
	case "DELETE":
		nt.RemoveTarget(id)
		w.WriteHeader(http.StatusNoContent)
	case "PUT":
		var target Target
		if err := json.NewDecoder(r.Body).Decode(&target); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		target.ID = id
		nt.AddTarget(&target)
		json.NewEncoder(w).Encode(target)
	}
}

func (nt *NetworkTester) handleConfig(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case "GET":
		json.NewEncoder(w).Encode(nt.GetConfig())
	case "POST", "PUT":
		var config Config
		if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		nt.SetConfig(config)
		json.NewEncoder(w).Encode(nt.GetConfig())
	}
}

func (nt *NetworkTester) handleResults(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(nt.GetResults())
}

func (nt *NetworkTester) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(nt.calculateStats())
}

// ==================== 主函数 ====================

func main() {
	tester := NewNetworkTester()

	r := mux.NewRouter()

	r.HandleFunc("/api/ws", tester.handleWebSocket)
	r.HandleFunc("/api/targets", tester.handleTargets).Methods("GET", "POST")
	r.HandleFunc("/api/targets/{id}", tester.handleTarget).Methods("PUT", "DELETE")
	r.HandleFunc("/api/config", tester.handleConfig).Methods("GET", "POST", "PUT")
	r.HandleFunc("/api/results", tester.handleResults).Methods("GET")
	r.HandleFunc("/api/stats", tester.handleStats).Methods("GET")

	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(indexHTML))
	})

	server := &http.Server{
		Addr:         ":8080",
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Println("Server starting on http://localhost:8080")
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down...")
	tester.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	server.Shutdown(ctx)
}

const indexHTML = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>网络测试工具</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%); color: #fff; min-height: 100vh; }
        .container { max-width: 1400px; margin: 0 auto; padding: 20px; }
        h1 { text-align: center; margin-bottom: 30px; font-size: 2rem; background: linear-gradient(90deg, #00d4ff, #7b2cbf); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin-bottom: 20px; }
        .card { background: rgba(255,255,255,0.05); border-radius: 16px; padding: 20px; backdrop-filter: blur(10px); border: 1px solid rgba(255,255,255,0.1); }
        .card h2 { font-size: 1rem; color: #888; margin-bottom: 15px; display: flex; align-items: center; gap: 8px; }
        .stat-value { font-size: 2.5rem; font-weight: bold; background: linear-gradient(90deg, #00d4ff, #7b2cbf); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
        .stat-unit { font-size: 0.9rem; color: #666; margin-left: 5px; }
        .stats-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 15px; }
        .stat-item { text-align: center; padding: 15px; background: rgba(0,0,0,0.2); border-radius: 12px; }
        .stat-item .label { font-size: 0.8rem; color: #888; margin-bottom: 5px; }
        .stat-item .value { font-size: 1.5rem; font-weight: bold; color: #00d4ff; }
        .chart-container { height: 300px; position: relative; }
        .controls { display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 15px; }
        input, select, button { padding: 10px 15px; border: none; border-radius: 8px; font-size: 0.9rem; }
        input, select { background: rgba(255,255,255,0.1); color: #fff; flex: 1; min-width: 150px; }
        input::placeholder { color: #666; }
        button { background: linear-gradient(90deg, #00d4ff, #7b2cbf); color: #fff; cursor: pointer; font-weight: bold; transition: transform 0.2s, opacity 0.2s; }
        button:hover { transform: translateY(-2px); opacity: 0.9; }
        button.danger { background: linear-gradient(90deg, #ff4757, #ff6b81); }
        button.success { background: linear-gradient(90deg, #2ed573, #7bed9f); }
        .target-list { max-height: 200px; overflow-y: auto; }
        .target-item { display: flex; align-items: center; justify-content: space-between; padding: 10px; background: rgba(0,0,0,0.2); border-radius: 8px; margin-bottom: 8px; }
        .target-item .name { font-weight: bold; }
        .target-item .url { font-size: 0.8rem; color: #888; word-break: break-all; }
        .target-item .actions { display: flex; gap: 5px; }
        .target-item button { padding: 5px 10px; font-size: 0.8rem; }
        .toggle { position: relative; width: 50px; height: 26px; }
        .toggle input { opacity: 0; width: 0; height: 0; }
        .toggle .slider { position: absolute; cursor: pointer; top: 0; left: 0; right: 0; bottom: 0; background: #333; border-radius: 26px; transition: 0.3s; }
        .toggle .slider:before { position: absolute; content: ""; height: 20px; width: 20px; left: 3px; bottom: 3px; background: #fff; border-radius: 50%; transition: 0.3s; }
        .toggle input:checked + .slider { background: linear-gradient(90deg, #00d4ff, #7b2cbf); }
        .toggle input:checked + .slider:before { transform: translateX(24px); }
        .status { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 8px; }
        .status.running { background: #2ed573; animation: pulse 1s infinite; }
        .status.stopped { background: #ff4757; }
        @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }
        .flow-estimate { display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px; }
        .flow-item { padding: 12px; background: rgba(0,0,0,0.2); border-radius: 8px; text-align: center; }
        .flow-item .period { font-size: 0.8rem; color: #888; }
        .flow-item .amount { font-size: 1.2rem; font-weight: bold; color: #7b2cbf; margin-top: 5px; }
        ::-webkit-scrollbar { width: 6px; }
        ::-webkit-scrollbar-track { background: rgba(0,0,0,0.2); border-radius: 3px; }
        ::-webkit-scrollbar-thumb { background: #7b2cbf; border-radius: 3px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🌐 网络测试工具</h1>
        
        <div class="grid">
            <div class="card">
                <h2><span class="status" id="statusIndicator"></span>测试控制</h2>
                <div class="controls">
                    <input type="number" id="threads" placeholder="线程数" min="1" max="100" value="4">
                    <button id="startBtn" class="success" onclick="toggleTest()">开始测试</button>
                </div>
                <div class="stats-grid">
                    <div class="stat-item">
                        <div class="label">活跃线程</div>
                        <div class="value" id="activeThreads">0</div>
                    </div>
                    <div class="stat-item">
                        <div class="label">测试次数</div>
                        <div class="value" id="testCount">0</div>
                    </div>
                    <div class="stat-item">
                        <div class="label">成功次数</div>
                        <div class="value" id="successCount">0</div>
                    </div>
                    <div class="stat-item">
                        <div class="label">丢包率 (近10秒/总)</div>
                        <div class="value" id="packetLoss">0%/0%</div>
                    </div>
                </div>
            </div>

            <div class="card">
                <h2>📍 测试地址</h2>
                <div class="controls">
                    <input type="text" id="targetName" placeholder="名称">
                    <input type="text" id="targetUrl" placeholder="URL (如 https://www.baidu.com)">
                    <button onclick="addTarget()">添加</button>
                </div>
                <div class="target-list" id="targetList"></div>
            </div>
        </div>

        <div class="grid">
            <div class="card">
                <h2>⚡ 当前吞吐 (收+发)</h2>
                <div class="stat-value" id="currentSpeed">0</div>
                <span class="stat-unit">KB/s</span>
            </div>
            <div class="card">
                <h2>📶 平均延迟</h2>
                <div class="stat-value" id="avgLatency">0</div>
                <span class="stat-unit">ms</span>
            </div>
            <div class="card">
                <h2>📊 总接收流量</h2>
                <div class="stat-value" id="totalRecv">0</div>
                <span class="stat-unit" id="totalRecvUnit">B</span>
            </div>
            <div class="card">
                <h2>📤 总发送流量</h2>
                <div class="stat-value" id="totalSent">0</div>
                <span class="stat-unit" id="totalSentUnit">B</span>
            </div>
        </div>

        <div class="grid">
            <div class="card" style="grid-column: span 2;">
                <h2>📈 实时监控</h2>
                <div class="chart-container">
                    <canvas id="chart"></canvas>
                </div>
            </div>
        </div>

        <div class="grid">
            <div class="card">
                <h2>💰 流量预估 (按当前速度)</h2>
                <div class="flow-estimate">
                    <div class="flow-item">
                        <div class="period">每分钟</div>
                        <div class="amount" id="perMinute">0 B</div>
                    </div>
                    <div class="flow-item">
                        <div class="period">每小时</div>
                        <div class="amount" id="perHour">0 B</div>
                    </div>
                    <div class="flow-item">
                        <div class="period">每天</div>
                        <div class="amount" id="perDay">0 B</div>
                    </div>
                    <div class="flow-item">
                        <div class="period">每月</div>
                        <div class="amount" id="perMonth">0 B</div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script>
        let ws;
        let isRunning = false;
        let chart;
        const maxDataPoints = 60;
        const chartData = {
            labels: [],
            speed: [],
            speedRaw: [],
            latency: [],
            packetLoss: []
        };
        let currentSpeedUnit = 'KB';

        function initChart() {
            const ctx = document.getElementById('chart').getContext('2d');
            chart = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: chartData.labels,
                    datasets: [
                        {
                            label: '吞吐 ',
                            data: chartData.speed,
                            borderColor: '#00d4ff',
                            backgroundColor: 'rgba(0, 212, 255, 0.1)',
                            fill: true,
                            tension: 0.4,
                            yAxisID: 'y'
                        },
                        {
                            label: '延迟 (ms)',
                            data: chartData.latency,
                            borderColor: '#7b2cbf',
                            backgroundColor: 'rgba(123, 44, 191, 0.1)',
                            fill: true,
                            tension: 0.4,
                            yAxisID: 'y1'
                        },
                        {
                            label: '丢包率 (%)',
                            data: chartData.packetLoss,
                            borderColor: '#ff4757',
                            backgroundColor: 'rgba(255, 71, 87, 0.1)',
                            fill: true,
                            tension: 0.4,
                            yAxisID: 'y2'
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: { intersect: false, mode: 'index' },
                    plugins: { legend: { labels: { color: '#fff' } } },
                    scales: {
                        x: { ticks: { color: '#888' }, grid: { color: 'rgba(255,255,255,0.1)' } },
                        y: { type: 'linear', position: 'left', ticks: { color: '#00d4ff' }, grid: { color: 'rgba(255,255,255,0.1)' } },
                        y1: { type: 'linear', position: 'right', ticks: { color: '#7b2cbf' }, grid: { display: false } },
                        y2: { type: 'linear', position: 'right', ticks: { color: '#ff4757' }, grid: { display: false }, min: 0, max: 100 }
                    }
                }
            });
        }

        function formatBytes(bytes) {
            if (bytes === 0) return { value: 0, unit: 'B' };
            const k = 1024;
            const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return { value: parseFloat((bytes / Math.pow(k, i)).toFixed(2)), unit: sizes[i] };
        }

        function formatBytesStr(bytes) {
            const f = formatBytes(bytes);
            return f.value + ' ' + f.unit;
        }

        function connectWebSocket() {
            const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
            ws = new WebSocket(protocol + '//' + location.host + '/api/ws');
            
            ws.onmessage = (event) => {
                const data = JSON.parse(event.data);
                if (data.type === 'stats') {
                    updateStats(data.stats);
                }
            };
            
            ws.onclose = () => setTimeout(connectWebSocket, 3000);
            ws.onerror = () => ws.close();
        }

        function updateStats(stats) {
            const speed = formatBytes(stats.currentSpeed);
            document.getElementById('currentSpeed').textContent = speed.value;
            document.querySelector('#currentSpeed + .stat-unit').textContent = speed.unit + '/s';
            
            document.getElementById('avgLatency').textContent = stats.avgLatency.toFixed(1);
            document.getElementById('packetLoss').textContent = stats.packetLossRecent.toFixed(1) + '%/' + stats.packetLoss.toFixed(1) + '%';
            document.getElementById('activeThreads').textContent = stats.activeThreads;
            document.getElementById('testCount').textContent = stats.testCount;
            document.getElementById('successCount').textContent = stats.successCount;
            
            const recv = formatBytes(stats.totalBytesRecv);
            document.getElementById('totalRecv').textContent = recv.value;
            document.getElementById('totalRecvUnit').textContent = recv.unit;
            
            const sent = formatBytes(stats.totalBytesSent);
            document.getElementById('totalSent').textContent = sent.value;
            document.getElementById('totalSentUnit').textContent = sent.unit;
            
            document.getElementById('perMinute').textContent = formatBytesStr(stats.perMinute);
            document.getElementById('perHour').textContent = formatBytesStr(stats.perHour);
            document.getElementById('perDay').textContent = formatBytesStr(stats.perDay);
            document.getElementById('perMonth').textContent = formatBytesStr(stats.perMonth);
            
            const now = new Date().toLocaleTimeString();
            chartData.labels.push(now);
            chartData.speedRaw.push(stats.currentSpeed);
            chartData.latency.push(stats.avgLatency.toFixed(1));
            chartData.packetLoss.push(stats.packetLossRecent.toFixed(1));
            
            if (chartData.labels.length > maxDataPoints) {
                chartData.labels.shift();
                chartData.speedRaw.shift();
                chartData.latency.shift();
                chartData.packetLoss.shift();
            }
            
            updateChartSpeed();
            chart.update('none');
        }

        function updateChartSpeed() {
            if (chartData.speedRaw.length === 0) return;
            const maxSpeed = Math.max(...chartData.speedRaw);
            let divisor = 1024;
            let unit = 'KB';
            if (maxSpeed >= 1024 * 1024 * 1024) {
                divisor = 1024 * 1024 * 1024;
                unit = 'GB';
            } else if (maxSpeed >= 1024 * 1024) {
                divisor = 1024 * 1024;
                unit = 'MB';
            } else if (maxSpeed >= 1024) {
                divisor = 1024;
                unit = 'KB';
            } else {
                divisor = 1;
                unit = 'B';
            }
            chartData.speed.length = 0;
            chartData.speedRaw.forEach(v => chartData.speed.push((v / divisor).toFixed(2)));
            if (currentSpeedUnit !== unit) {
                currentSpeedUnit = unit;
                chart.data.datasets[0].label = '吞吐  ' + unit + '/s';
            }
        }

        async function loadTargets() {
            const resp = await fetch('/api/targets');
            const targets = await resp.json();
            const list = document.getElementById('targetList');
            list.innerHTML = targets.map(t => ` + "`" + `
                <div class="target-item">
                    <div>
                        <div class="name">${t.name || '未命名'}</div>
                        <div class="url">${t.url}</div>
                    </div>
                    <div class="actions">
                        <label class="toggle">
                            <input type="checkbox" ${t.enabled ? 'checked' : ''} onchange="toggleTarget('${t.id}', this.checked)">
                            <span class="slider"></span>
                        </label>
                        <button class="danger" onclick="deleteTarget('${t.id}')">删除</button>
                    </div>
                </div>
            ` + "`" + `).join('');
        }

        async function addTarget() {
            const name = document.getElementById('targetName').value;
            const url = document.getElementById('targetUrl').value;
            if (!url) return alert('请输入URL');
            
            await fetch('/api/targets', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ name, url, enabled: true })
            });
            
            document.getElementById('targetName').value = '';
            document.getElementById('targetUrl').value = '';
            loadTargets();
        }

        async function deleteTarget(id) {
            await fetch('/api/targets/' + id, { method: 'DELETE' });
            loadTargets();
        }

        async function toggleTarget(id, enabled) {
            const resp = await fetch('/api/targets');
            const targets = await resp.json();
            const target = targets.find(t => t.id === id);
            if (target) {
                target.enabled = enabled;
                await fetch('/api/targets/' + id, {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(target)
                });
            }
        }

        async function loadConfig() {
            const resp = await fetch('/api/config');
            const config = await resp.json();
            document.getElementById('threads').value = config.threads;
            isRunning = config.running;
            updateUI();
        }

        async function toggleTest() {
            const threads = parseInt(document.getElementById('threads').value) || 4;
            
            await fetch('/api/config', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ threads, running: !isRunning })
            });
            
            isRunning = !isRunning;
            updateUI();
        }

        function updateUI() {
            const btn = document.getElementById('startBtn');
            const indicator = document.getElementById('statusIndicator');
            if (isRunning) {
                btn.textContent = '停止测试';
                btn.className = 'danger';
                indicator.className = 'status running';
            } else {
                btn.textContent = '开始测试';
                btn.className = 'success';
                indicator.className = 'status stopped';
            }
        }

        initChart();
        connectWebSocket();
        loadTargets();
        loadConfig();
    </script>
</body>
</html>
` + "`"
