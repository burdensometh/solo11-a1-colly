package queue

import (
	"bytes"
	"errors"
	"log"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gocolly/colly/v2"
)

func TestQueue(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(serverHandler))
	defer server.Close()

	rng := rand.New(rand.NewSource(12387123712321232))
	var rngMu sync.Mutex

	var (
		items    uint32
		requests uint32
		success  uint32
		failure  uint32
	)
	storage := &InMemoryQueueStorage{MaxSize: 100000}
	q, err := New(10, storage)
	if err != nil {
		panic(err)
	}
	put := func() {
		rngMu.Lock()
		t := time.Duration(rng.Intn(50)) * time.Microsecond
		rngMu.Unlock()
		url := server.URL + "/delay?t=" + t.String()
		atomic.AddUint32(&items, 1)
		q.AddURL(url)
	}
	for i := 0; i < 3000; i++ {
		put()
		storage.AddRequest([]byte("error request"))
	}
	c := colly.NewCollector(
		colly.AllowURLRevisit(),
	)
	c.OnRequest(func(req *colly.Request) {
		atomic.AddUint32(&requests, 1)
	})
	c.OnResponse(func(resp *colly.Response) {
		if resp.StatusCode == http.StatusOK {
			atomic.AddUint32(&success, 1)
		} else {
			atomic.AddUint32(&failure, 1)
		}
		rngMu.Lock()
		toss := rng.Intn(2) == 0
		rngMu.Unlock()
		if toss {
			put()
		}
	})
	c.OnError(func(resp *colly.Response, err error) {
		atomic.AddUint32(&failure, 1)
	})
	err = q.Run(c)
	if err != nil {
		t.Fatalf("Queue.Run() return an error: %v", err)
	}
	if items != requests || success+failure != requests || failure > 0 {
		t.Fatalf("wrong Queue implementation: "+
			"items = %d, requests = %d, success = %d, failure = %d",
			items, requests, success, failure)
	}
}

func TestMemoryStorageBlocksWhenFull(t *testing.T) {
	storage := &InMemoryQueueStorage{MaxSize: 2}
	if err := storage.Init(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		if err := storage.AddRequest([]byte("req")); err != nil {
			t.Fatalf("AddRequest() #%d returned an error: %v", i, err)
		}
	}

	added := make(chan error, 1)
	go func() { added <- storage.AddRequest([]byte("third")) }()

	select {
	case err := <-added:
		t.Fatalf("AddRequest() returned before there was room: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Consuming one request must unblock the producer instead of dropping it.
	if _, err := storage.GetRequest(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-added:
		if err != nil {
			t.Fatalf("AddRequest() returned an error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("AddRequest() did not unblock after GetRequest()")
	}

	if size, _ := storage.QueueSize(); size != 2 {
		t.Fatalf("QueueSize() = %d, want 2", size)
	}
	if storage.BlockedDuration() <= 0 {
		t.Fatal("BlockedDuration() = 0, want > 0")
	}

	// Blocked time is only counted when the queue was actually full.
	if _, err := storage.GetRequest(); err != nil {
		t.Fatal(err)
	}
	blockedBefore := storage.BlockedDuration()
	start := time.Now()
	if err := storage.AddRequest([]byte("fits")); err != nil {
		t.Fatalf("AddRequest() returned an error: %v", err)
	}
	if storage.BlockedDuration() != blockedBefore {
		t.Fatal("BlockedDuration() grew although the queue was not full")
	}
	if time.Since(start) > 50*time.Millisecond {
		t.Fatal("AddRequest() blocked although the queue was not full")
	}
}

func TestMemoryStorageCloseUnblocksProducer(t *testing.T) {
	storage := &InMemoryQueueStorage{MaxSize: 1}
	if err := storage.Init(); err != nil {
		t.Fatal(err)
	}
	if err := storage.AddRequest([]byte("first")); err != nil {
		t.Fatal(err)
	}

	added := make(chan error, 1)
	go func() { added <- storage.AddRequest([]byte("second")) }()
	time.Sleep(20 * time.Millisecond)

	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-added:
		if !errors.Is(err, ErrQueueClosed) {
			t.Fatalf("AddRequest() error = %v, want ErrQueueClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("AddRequest() still blocked after Close()")
	}

	// The already queued request can still be drained.
	buf, err := storage.GetRequest()
	if err != nil || string(buf) != "first" {
		t.Fatalf("GetRequest() = %q, %v; want \"first\", nil", buf, err)
	}
}

func TestMemoryStoragePreservesOrder(t *testing.T) {
	storage := &InMemoryQueueStorage{MaxSize: 4}
	if err := storage.Init(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if err := storage.AddRequest([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 4; i++ {
		buf, err := storage.GetRequest()
		if err != nil || len(buf) != 1 || buf[0] != byte(i) {
			t.Fatalf("GetRequest() #%d = %v, %v; want [%d]", i, buf, err, i)
		}
		// Interleave an add so the ring buffer wraps around.
		if err := storage.AddRequest([]byte{byte(i + 4)}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 4; i++ {
		buf, err := storage.GetRequest()
		if err != nil || len(buf) != 1 || buf[0] != byte(i+4) {
			t.Fatalf("GetRequest() #%d = %v, %v; want [%d]", i, buf, err, i+4)
		}
	}
}

func TestQueueDrainsOnStop(t *testing.T) {
	var visited uint32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddUint32(&visited, 1)
		time.Sleep(5 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	q, err := New(2, &InMemoryQueueStorage{MaxSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	q.MetricsInterval = time.Hour
	q.ShutdownTimeout = 5 * time.Second

	const total = 20
	for i := 0; i < total; i++ {
		if err := q.AddURL(server.URL); err != nil {
			t.Fatalf("AddURL() returned an error: %v", err)
		}
	}

	var once sync.Once
	c := colly.NewCollector(colly.AllowURLRevisit())
	c.OnResponse(func(r *colly.Response) {
		// Stop right after the first response: Run must still drain the rest.
		once.Do(q.Stop)
	})

	if err := q.Run(c); err != nil {
		t.Fatalf("Run() returned an error: %v", err)
	}
	if got := atomic.LoadUint32(&visited); got != total {
		t.Fatalf("visited %d URLs after Stop, want all %d", got, total)
	}
	if stats := q.Stats(); stats.Consumed != total {
		t.Fatalf("Stats().Consumed = %d, want %d", stats.Consumed, total)
	}
	if stats := q.Stats(); stats.Backlog != 0 {
		t.Fatalf("Stats().Backlog = %d, want 0", stats.Backlog)
	}
}

func TestQueueStopTimeout(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	defer close(release)

	q, err := New(1, &InMemoryQueueStorage{MaxSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	q.MetricsInterval = time.Hour
	q.ShutdownTimeout = 100 * time.Millisecond

	c := colly.NewCollector(colly.AllowURLRevisit())
	c.OnRequest(func(r *colly.Request) {
		q.Stop()
	})
	for i := 0; i < 3; i++ {
		if err := q.AddURL(server.URL); err != nil {
			t.Fatal(err)
		}
	}

	start := time.Now()
	err = q.Run(c)
	elapsed := time.Since(start)
	if !errors.Is(err, ErrShutdownTimeout) {
		t.Fatalf("Run() error = %v, want ErrShutdownTimeout", err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("Run() took %s, want it to give up near ShutdownTimeout", elapsed)
	}
}

func TestQueueAddAfterStop(t *testing.T) {
	q, err := New(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	q.MetricsInterval = time.Hour
	q.Stop()

	// A stopped queue rejects new requests rather than blocking forever.
	done := make(chan error, 1)
	go func() { done <- q.AddURL("http://example.com") }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrQueueClosed) {
			t.Fatalf("AddURL() error = %v, want ErrQueueClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("AddURL() blocked on a stopped queue")
	}
}

func TestQueueBackpressureWithoutLoss(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	const capacity = 3
	const total = 50
	q, err := New(2, &InMemoryQueueStorage{MaxSize: capacity})
	if err != nil {
		t.Fatal(err)
	}
	q.MetricsInterval = time.Hour
	q.ShutdownTimeout = 10 * time.Second

	// Fill the queue before Run starts: producers must block, not drop.
	for i := 0; i < capacity; i++ {
		if err := q.AddURL(server.URL); err != nil {
			t.Fatal(err)
		}
	}
	var queued int32
	atomic.StoreInt32(&queued, capacity)

	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		for i := capacity; i < total; i++ {
			if err := q.AddURL(server.URL); err != nil {
				t.Errorf("AddURL() returned an error: %v", err)
				return
			}
			atomic.AddInt32(&queued, 1)
		}
	}()

	// The producer must be blocked while nothing consumes the queue.
	time.Sleep(50 * time.Millisecond)
	if got := atomic.LoadInt32(&queued); got != capacity {
		t.Fatalf("queued %d requests into a queue with capacity %d", got, capacity)
	}
	if size, _ := q.Size(); size > capacity {
		t.Fatalf("Size() = %d, exceeds MaxSize %d", size, capacity)
	}

	var visited uint32
	c := colly.NewCollector(colly.AllowURLRevisit())
	c.OnResponse(func(r *colly.Response) {
		atomic.AddUint32(&visited, 1)
	})
	if err := q.Run(c); err != nil {
		t.Fatalf("Run() returned an error: %v", err)
	}
	<-producerDone
	if got := atomic.LoadUint32(&visited); got != total {
		t.Fatalf("visited %d URLs, want %d (requests were dropped)", got, total)
	}
	if stats := q.Stats(); stats.Blocked <= 0 {
		t.Fatal("Stats().Blocked = 0, want > 0 with a full queue")
	}
}

func TestQueueMetricsLogging(t *testing.T) {
	var buf bytes.Buffer
	q, err := New(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	q.MetricsInterval = 10 * time.Millisecond
	q.Logger = log.New(&buf, "", 0)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		q.reportMetrics(stop)
		close(done)
	}()
	time.Sleep(60 * time.Millisecond)
	close(stop)
	<-done

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) < 2 {
		t.Fatalf("got %d metric lines, want at least 2:\n%s", len(lines), buf.String())
	}
	for _, line := range lines {
		for _, field := range []string{"backlog=", "rate=", "blocked="} {
			if !strings.Contains(line, field) {
				t.Fatalf("metric line %q is missing %q", line, field)
			}
		}
	}
}

func TestStatsConsumed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	q, err := New(3, &InMemoryQueueStorage{MaxSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	q.MetricsInterval = time.Hour

	const total = 10
	for i := 0; i < total; i++ {
		if err := q.AddURL(server.URL); err != nil {
			t.Fatal(err)
		}
	}
	c := colly.NewCollector(colly.AllowURLRevisit())
	if err := q.Run(c); err != nil {
		t.Fatalf("Run() returned an error: %v", err)
	}
	if stats := q.Stats(); stats.Consumed != total {
		t.Fatalf("Stats().Consumed = %d, want %d", stats.Consumed, total)
	}
}

func serverHandler(w http.ResponseWriter, req *http.Request) {
	if !serverRoute(w, req) {
		shutdown(w)
	}
}

func serverRoute(w http.ResponseWriter, req *http.Request) bool {
	if req.URL.Path == "/delay" {
		return serveDelay(w, req) == nil
	}
	return false
}

func serveDelay(w http.ResponseWriter, req *http.Request) error {
	q := req.URL.Query()
	t, err := time.ParseDuration(q.Get("t"))
	if err != nil {
		return err
	}
	time.Sleep(t)
	w.WriteHeader(http.StatusOK)
	return nil
}

func shutdown(w http.ResponseWriter) {
	taker, ok := w.(http.Hijacker)
	if !ok {
		return
	}
	raw, _, err := taker.Hijack()
	if err != nil {
		return
	}
	raw.Close()
}
