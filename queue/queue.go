package queue

import (
	"errors"
	"log"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	whatwgUrl "github.com/nlnwa/whatwg-url/url"

	"github.com/gocolly/colly/v2"
)

var urlParser = whatwgUrl.NewParser(whatwgUrl.WithPercentEncodeSinglePercentSign())

var (
	// ErrQueueClosed is returned when a request is added to a queue
	// which is shutting down or has already been shut down.
	ErrQueueClosed = errors.New("queue is closed")
	// ErrShutdownTimeout is returned by Run when a Stop was requested but the
	// queue could not be drained within Queue.ShutdownTimeout.
	ErrShutdownTimeout = errors.New("queue shutdown timed out waiting for the queue to drain")
)

const defaultShutdownTimeout = 30 * time.Second

// Storage is the interface of the queue's storage backend
// Storage must be concurrently safe for multiple goroutines.
type Storage interface {
	// Init initializes the storage
	Init() error
	// AddRequest adds a serialized request to the queue.
	// Implementations may block until there is room in the queue and should
	// return ErrQueueClosed once the storage has been closed.
	AddRequest([]byte) error
	// GetRequest pops the next request from the queue
	// or returns error if the queue is empty
	GetRequest() ([]byte, error)
	// QueueSize returns with the size of the queue
	QueueSize() (int, error)
}

// Closer is implemented by storages which need to release resources or
// unblock waiting producers when the queue stops.
type Closer interface {
	Close() error
}

// Stats is a snapshot of the queue's runtime statistics.
type Stats struct {
	// Backlog is the number of requests waiting to be consumed.
	Backlog int
	// Consumed is the total number of requests processed by consumer threads.
	Consumed uint64
	// Blocked is the total time producers spent waiting for room in the queue.
	Blocked time.Duration
}

// Queue is a request queue which uses a Collector to consume
// requests in multiple threads
type Queue struct {
	// Threads defines the number of consumer threads
	Threads int
	// ShutdownTimeout is the maximum amount of time Stop waits for the
	// queue to drain. If it is zero or negative, Run waits indefinitely.
	ShutdownTimeout time.Duration
	// MetricsInterval controls how often the queue logs its backlog,
	// consumption rate and blocked duration. Zero defaults to one second.
	MetricsInterval time.Duration
	// Logger is used by the metrics reporter. A nil Logger uses log.Default().
	Logger *log.Logger

	storage Storage
	wake    chan struct{}
	// stopped is closed exactly once by Stop. The dispatcher watches it so a
	// stop request starts the shutdown timeout even while it is blocked
	// waiting for a free consumer.
	stopped  chan struct{}
	stopOnce sync.Once
	mut      sync.Mutex // guards wake, running and closed
	running  bool
	closed   bool

	// gate keeps AddRequest from interleaving with the dispatcher's decision
	// to terminate. Producers hold it for reading while storing a request;
	// the dispatcher takes it for writing before declaring the queue drained,
	// so a request can never be enqueued after Run has decided to return.
	gate sync.RWMutex

	consumed atomic.Uint64
}

// InMemoryQueueStorage is the default implementation of the Storage interface.
// InMemoryQueueStorage holds the request queue in memory.
type InMemoryQueueStorage struct {
	// blockedNanos is kept first to guarantee 64-bit alignment for atomics
	// on 32-bit platforms.
	blockedNanos int64

	// MaxSize defines the fixed capacity of the queue.
	// AddRequest blocks while the queue is full instead of discarding requests.
	// A non-positive MaxSize means the queue is unbounded.
	MaxSize int

	lock   *sync.Mutex
	cond   *sync.Cond
	size   int
	first  int
	buf    [][]byte
	closed bool
}

// New creates a new queue with a Storage specified in argument
// A standard InMemoryQueueStorage is used if Storage argument is nil.
func New(threads int, s Storage) (*Queue, error) {
	if s == nil {
		s = &InMemoryQueueStorage{MaxSize: 100000}
	}
	if err := s.Init(); err != nil {
		return nil, err
	}
	return &Queue{
		Threads:         threads,
		ShutdownTimeout: defaultShutdownTimeout,
		MetricsInterval: time.Second,
		storage:         s,
		running:         true,
	}, nil
}

// IsEmpty returns true if the queue is empty
func (q *Queue) IsEmpty() bool {
	s, _ := q.Size()
	return s == 0
}

// AddURL adds a new URL to the queue.
// If the queue is full, AddURL blocks until there is room or the queue is
// stopped, in which case ErrQueueClosed is returned.
func (q *Queue) AddURL(URL string) error {
	u, err := urlParser.Parse(URL)
	if err != nil {
		return err
	}
	u2, err := url.Parse(u.Href(false))
	if err != nil {
		return err
	}
	r := &colly.Request{
		URL:    u2,
		Method: "GET",
	}
	return q.AddRequest(r)
}

// AddRequest adds a new Request to the queue.
// If the queue is full, AddRequest blocks until there is room or the queue is
// stopped, in which case ErrQueueClosed is returned.
func (q *Queue) AddRequest(r *colly.Request) error {
	q.gate.RLock()
	defer q.gate.RUnlock()

	q.mut.Lock()
	closed := q.wake != nil && (!q.running || q.closed)
	q.mut.Unlock()
	if closed {
		return ErrQueueClosed
	}
	if err := q.storeRequest(r); err != nil {
		return err
	}
	q.signal()
	return nil
}

func (q *Queue) storeRequest(r *colly.Request) error {
	d, err := r.Marshal()
	if err != nil {
		return err
	}
	return q.storage.AddRequest(d)
}

// signal wakes the dispatcher if it is waiting for new requests.
func (q *Queue) signal() {
	q.mut.Lock()
	wake, running := q.wake, q.running
	q.mut.Unlock()
	if wake == nil || !running {
		return
	}
	select {
	case wake <- struct{}{}:
	default:
	}
}

// Size returns the size of the queue
func (q *Queue) Size() (int, error) {
	return q.storage.QueueSize()
}

// Stats returns a snapshot of the queue's runtime statistics.
func (q *Queue) Stats() Stats {
	size, _ := q.storage.QueueSize()
	stats := Stats{
		Backlog:  size,
		Consumed: q.consumed.Load(),
	}
	if r, ok := q.storage.(interface{ BlockedDuration() time.Duration }); ok {
		stats.Blocked = r.BlockedDuration()
	}
	return stats
}

// Run starts consumer threads and calls the Collector
// to perform requests. Run blocks while the queue has active requests.
// When Stop is called, Run keeps consuming queued requests and returns once
// the queue is drained or ShutdownTimeout expires (ErrShutdownTimeout).
// The given Storage must not be used directly while Run blocks.
func (q *Queue) Run(c *colly.Collector) error {
	q.mut.Lock()
	if q.wake != nil {
		q.mut.Unlock()
		panic("cannot call duplicate Queue.Run")
	}
	q.wake = make(chan struct{}, 1)
	q.stopped = make(chan struct{})
	q.stopOnce = sync.Once{}
	q.running = true
	q.closed = false
	q.mut.Unlock()

	requestc := make(chan *colly.Request)
	complete := make(chan struct{})
	done := make(chan struct{})
	stopMetrics := make(chan struct{})
	metricsDone := make(chan struct{})
	errc := make(chan error, 1)

	for i := 0; i < q.Threads; i++ {
		go independentRunner(requestc, complete, done, &q.consumed)
	}
	go func() {
		defer close(metricsDone)
		q.reportMetrics(stopMetrics)
	}()
	go q.loop(c, requestc, complete, done, errc)

	err := <-errc

	// Unblock runners and producers. Metrics stop once Run is over.
	close(stopMetrics)
	<-metricsDone
	close(done)
	close(requestc)

	q.mut.Lock()
	q.running = false
	q.closed = true
	stopped := q.stopped
	q.mut.Unlock()

	if stopped != nil {
		q.stopOnce.Do(func() { close(stopped) })
	}

	if closer, ok := q.storage.(Closer); ok {
		closer.Close()
	}
	return err
}

// Stop will stop accepting new requests and let Run drain the queue.
// Requests blocked on a full queue are unblocked with ErrQueueClosed.
func (q *Queue) Stop() {
	q.mut.Lock()
	q.running = false
	wake, stopped := q.wake, q.stopped
	q.mut.Unlock()

	q.stopOnce.Do(func() {
		if stopped != nil {
			close(stopped)
		}
	})

	if closer, ok := q.storage.(Closer); ok {
		closer.Close()
	}

	// Wake the dispatcher so it notices the stop request even if it is
	// waiting for a new request to arrive.
	if wake != nil {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

func (q *Queue) loop(c *colly.Collector, requestc chan<- *colly.Request, complete <-chan struct{}, done <-chan struct{}, errc chan<- error) {
	var (
		active   int
		stopping bool
		timeoutC <-chan time.Time
		timer    *time.Timer
	)
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for {
		q.mut.Lock()
		running, shutdownTimeout, stopped := q.running, q.ShutdownTimeout, q.stopped
		q.mut.Unlock()

		if !running && !stopping {
			stopping = true
			if shutdownTimeout > 0 {
				timer = time.NewTimer(shutdownTimeout)
				timeoutC = timer.C
			}
		}

		// stoppedC is nil once the dispatcher has handled the stop signal, so
		// the (closed) channel does not keep firing in the select below.
		stoppedC := stopped
		size, err := q.storage.QueueSize()
		if err != nil {
			errc <- err
			return
		}
		if size == 0 && active == 0 {
			// Terminate when
			//   1. No active requests
			//   2. Empty queue
			//
			// Hold the gate for writing while deciding, so that no producer
			// can enqueue a request which would never be consumed. Any
			// producer waiting for the gate will observe running == false
			// and get ErrQueueClosed.
			q.gate.Lock()
			q.mut.Lock()
			size, err = q.storage.QueueSize()
			if err == nil && size > 0 {
				// A producer got in between the check and the lock.
				q.mut.Unlock()
				q.gate.Unlock()
				continue
			}
			q.running = false
			q.closed = true
			q.mut.Unlock()
			q.gate.Unlock()
			if err != nil {
				errc <- err
				return
			}
			errc <- nil
			return
		}

		sent := requestc
		var req *colly.Request
		if size > 0 {
			req, err = q.loadRequest(c)
			if err != nil {
				// Ignore an error returned by GetRequest() or
				// UnmarshalRequest() and keep draining the queue.
				continue
			}
		} else {
			sent = nil
		}

	Dispatch:
		for {
			select {
			case sent <- req:
				active++
				break Dispatch
			case <-q.wake:
				if sent == nil {
					// The queue may have grown, re-check its size.
					break Dispatch
				}
			case <-stoppedC:
				// Stop was called while the dispatcher was waiting for a
				// free consumer: start the shutdown timer right away.
				stoppedC = nil
				if !stopping {
					stopping = true
					if shutdownTimeout > 0 {
						timer = time.NewTimer(shutdownTimeout)
						timeoutC = timer.C
					}
				}
				if sent == nil {
					break Dispatch
				}
			case <-complete:
				active--
				if sent == nil && active <= 0 {
					// Nothing left to dispatch and no in-flight requests:
					// return to the top of the loop and re-check the queue.
					break Dispatch
				}
			case <-timeoutC:
				errc <- ErrShutdownTimeout
				return
			case <-done:
				// Run is already finishing, don't block the dispatcher.
				return
			}
		}
	}
}

func independentRunner(requestc <-chan *colly.Request, complete chan<- struct{}, done <-chan struct{}, consumed *atomic.Uint64) {
	for req := range requestc {
		req.Do()
		consumed.Add(1)
		select {
		case complete <- struct{}{}:
		case <-done:
		}
	}
}

// reportMetrics logs one line per MetricsInterval with the current backlog,
// the consumption rate and how long producers were blocked on a full queue.
func (q *Queue) reportMetrics(stop <-chan struct{}) {
	interval := q.MetricsInterval
	if interval <= 0 {
		interval = time.Second
	}
	logger := q.Logger
	if logger == nil {
		logger = log.Default()
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	last := time.Now()
	var lastConsumed uint64
	for {
		select {
		case <-stop:
			return
		case now := <-ticker.C:
			stats := q.Stats()
			elapsed := now.Sub(last).Seconds()
			var rate float64
			if elapsed > 0 {
				rate = float64(stats.Consumed-lastConsumed) / elapsed
			}
			logger.Printf("queue: backlog=%d rate=%.1f/s blocked=%s",
				stats.Backlog, rate, stats.Blocked)
			last, lastConsumed = now, stats.Consumed
		}
	}
}

func (q *Queue) loadRequest(c *colly.Collector) (*colly.Request, error) {
	buf, err := q.storage.GetRequest()
	if err != nil {
		return nil, err
	}
	copied := make([]byte, len(buf))
	copy(copied, buf)
	return c.UnmarshalRequest(copied)
}

// Init implements Storage.Init() function
func (q *InMemoryQueueStorage) Init() error {
	q.lock = &sync.Mutex{}
	q.cond = sync.NewCond(q.lock)
	q.size = 0
	q.first = 0
	q.closed = false
	q.buf = nil
	return nil
}

// AddRequest implements Storage.AddRequest() function.
// It blocks while the queue is at capacity instead of discarding requests.
func (q *InMemoryQueueStorage) AddRequest(r []byte) error {
	q.lock.Lock()
	defer q.lock.Unlock()

	var (
		blocked  bool
		waitFrom time.Time
	)
	for q.MaxSize > 0 && q.size >= q.MaxSize && !q.closed {
		if !blocked {
			blocked, waitFrom = true, time.Now()
		}
		q.cond.Wait()
	}
	if blocked {
		atomic.AddInt64(&q.blockedNanos, int64(time.Since(waitFrom)))
	}
	if q.closed {
		return ErrQueueClosed
	}

	if q.size == len(q.buf) {
		q.grow()
	}
	q.buf[(q.first+q.size)%len(q.buf)] = r
	q.size++
	q.cond.Broadcast()
	return nil
}

// grow enlarges the ring buffer, keeping its length at or below MaxSize.
// The caller guarantees that there is spare capacity, i.e. size < MaxSize.
func (q *InMemoryQueueStorage) grow() {
	newLen := len(q.buf) * 2
	if newLen < 16 {
		newLen = 16
	}
	if q.MaxSize > 0 && newLen > q.MaxSize && q.MaxSize > q.size {
		newLen = q.MaxSize
	}
	buf := make([][]byte, newLen)
	for i := 0; i < q.size; i++ {
		buf[i] = q.buf[(q.first+i)%len(q.buf)]
	}
	q.buf = buf
	q.first = 0
}

// GetRequest implements Storage.GetRequest() function
func (q *InMemoryQueueStorage) GetRequest() ([]byte, error) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if q.size == 0 {
		return nil, nil
	}
	r := q.buf[q.first]
	q.buf[q.first] = nil // release the reference held by the queue
	q.size--
	q.first = (q.first + 1) % len(q.buf)
	q.cond.Broadcast()
	return r, nil
}

// QueueSize implements Storage.QueueSize() function
func (q *InMemoryQueueStorage) QueueSize() (int, error) {
	q.lock.Lock()
	defer q.lock.Unlock()
	return q.size, nil
}

// Close implements Closer. It rejects future AddRequest calls, unblocks
// producers waiting on a full queue, and lets GetRequest drain what is left.
func (q *InMemoryQueueStorage) Close() error {
	q.lock.Lock()
	q.closed = true
	q.cond.Broadcast()
	q.lock.Unlock()
	return nil
}

// BlockedDuration returns the total time producers spent waiting for room.
func (q *InMemoryQueueStorage) BlockedDuration() time.Duration {
	return time.Duration(atomic.LoadInt64(&q.blockedNanos))
}
