package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"sync/atomic"
	"time"
)

const (
	MaxConcurrentTasksPerRequest = 4
	MaxConcurrentClients         = 100
	MaxUrlsPerRequest            = 20
)

// Lock-free client limiter
type ClientLimiter struct {
	MaxConcurrentClients int32
	clientCount          int32
}

func (c *ClientLimiter) Acquire() error {
	do := func() (bool, error) {
		current := atomic.LoadInt32(&c.clientCount)
		if current >= c.MaxConcurrentClients {
			return false, fmt.Errorf("limit reached")
		}

		ret := atomic.CompareAndSwapInt32(&c.clientCount, current, current+1)
		return ret, nil
	}

	for {
		acquired, err := do()
		if err != nil {
			return err
		}

		if acquired {
			return nil
		}
	}
}

func (c *ClientLimiter) release() {
	atomic.AddInt32(&c.clientCount, -1)
}

func readRequest(r io.Reader) ([]string, error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	var request struct {
		Urls []string `json:"urls"`
	}

	if err := json.Unmarshal(data, &request); err != nil {
		return nil, err
	}

	return request.Urls, nil
}

func jsonResponse(w http.ResponseWriter, data interface{}) {
	respBytes, err := json.Marshal(data)
	if err != nil {
		log.Panicf("Failed to marshall response to json: %s", err)
	}

	if _, err := w.Write(respBytes); err != nil {
		log.Printf("Failed to write response to client: %s", err)
	}
}

type Handler struct {
	client  *http.Client
	limiter ClientLimiter
}

func (h *Handler) onRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(400)

		jsonResponse(w, map[string]interface{}{
			"success": false,
			"reason":  "Method not supported",
		})
		return
	}

	if err := h.limiter.Acquire(); err != nil {
		w.WriteHeader(503)

		jsonResponse(w, map[string]interface{}{
			"success": false,
			"reason":  "Max parallel requests reached",
		})
		return
	}
	defer h.limiter.release()

	urls, err := readRequest(r.Body)
	if err != nil {
		log.Printf("Failed to read request: %s", err)
		jsonResponse(w, map[string]interface{}{
			"success": false,
			"reason":  err.Error(),
		})
		return
	}

	if len(urls) > MaxUrlsPerRequest {
		jsonResponse(w, map[string]interface{}{
			"success": false,
			"reason":  "Number of urls exceeds the maximum",
		})
		return
	}

	ret, err := downloadUrls(r.Context(), h.client, urls)
	if err != nil {
		jsonResponse(w, map[string]interface{}{
			"success": false,
			"reason":  err.Error(),
		})
		return
	}

	jsonResponse(w, map[string]interface{}{
		"success": true,
		"result": ret,
	})
}

type TaskResult struct {
	Url    string `json:"url"`
	Result string `json:"result"`
	Err    error  `json:"err"`
}

func downloadUrl(ctx context.Context, client *http.Client, url string) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := client.Do(request)
	if err != nil {
		return nil, err
	}

	if !(resp.StatusCode >= 200 && resp.StatusCode < 300) {
		errorData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("status code: %d", resp.StatusCode)
		}

		return nil, fmt.Errorf("status code: %d (%s)", resp.StatusCode, string(errorData))
	}

	ret, err := ioutil.ReadAll(resp.Body)
	return ret, err
}

func downloadUrls(ctx context.Context, client *http.Client, urls []string) ([]TaskResult, error) {
	ctx, cancelRequests := context.WithCancel(ctx)

	worker := func(tasks chan string, results chan TaskResult) {
		for url := range tasks {
			ret, err := downloadUrl(ctx, client, url)
			if err != nil {
				log.Printf("Failed to process Url \"%s\" : %s", url, err)
			}

			results <- TaskResult{Err: err, Result: string(ret), Url: url}
		}
	}

	tasks := make(chan string, len(urls))
	for _, url := range urls {
		tasks <- url
	}
	close(tasks)

	results := make(chan TaskResult, len(urls))
	for i := 0; i < MaxConcurrentTasksPerRequest; i++ {
		go worker(tasks, results)
	}

	ret := make([]TaskResult, 0, len(urls))
	for i := 0; i < len(urls); i++ {
		result := <-results

		if result.Err != nil {
			cancelRequests()
			return nil, fmt.Errorf("failed to download Url \"%s\": %s", result.Url, result.Err)
		}

		ret = append(ret, result)
	}

	return ret, nil
}

func main() {
	h := Handler{
		limiter: ClientLimiter{MaxConcurrentClients, 0},
		client: &http.Client{
			Timeout: 1 * time.Second,
		},
	}

	http.HandleFunc("/", h.onRequest)

	log.Println("Listen on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
