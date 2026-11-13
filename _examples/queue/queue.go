package main

import (
	"fmt"
	"time"

	"github.com/gocolly/colly/v2"
	"github.com/gocolly/colly/v2/queue"
)

func main() {
	url := "https://httpbin.org/delay/1"

	// Instantiate default collector
	c := colly.NewCollector(colly.AllowURLRevisit())

	// create a request queue with 2 consumer threads and a bounded in-memory
	// storage. When the queue is full, AddURL/AddRequest block until a
	// consumer frees a slot instead of dropping the request.
	q, _ := queue.New(
		2, // Number of consumer threads
		&queue.InMemoryQueueStorage{MaxSize: 10000},
	)

	// The queue logs backlog, consumption rate and producer blocking time
	// every second by default.
	q.MetricsInterval = time.Second

	c.OnRequest(func(r *colly.Request) {
		fmt.Println("visiting", r.URL)
		if r.ID < 15 {
			r2, err := r.New("GET", fmt.Sprintf("%s?x=%v", url, r.ID), nil)
			if err == nil {
				// Blocks only if the queue is at capacity.
				q.AddRequest(r2)
			}
		}
	})

	for i := 0; i < 5; i++ {
		// Add URLs to the queue
		q.AddURL(fmt.Sprintf("%s?n=%d", url, i))
	}
	// Consume URLs. Run drains the queue and returns once it is empty.
	// Stop (e.g. from a signal handler) makes Run finish the queued requests
	// and give up after q.ShutdownTimeout, returning queue.ErrShutdownTimeout.
	q.Run(c)

	stats := q.Stats()
	fmt.Printf("done: consumed=%d backlog=%d blocked=%s\n",
		stats.Consumed, stats.Backlog, stats.Blocked)
}
