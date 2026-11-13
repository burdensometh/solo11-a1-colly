// Copyright 2018 Adam Tauber
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package colly

import (
	"hash/fnv"
	"net/url"
	"sync"

	"github.com/gocolly/colly/v2/storage"
)

// cookieLockShards is the number of locks guarding cookie read-modify-write
// cycles. Cookie data is scoped by host, so requests to different hosts use
// different shards and do not serialize on each other.
const cookieLockShards = 32

// concurrentStorage makes a storage.Storage safe for concurrent use by a
// Collector.
//
// The Storage interface has no combined check-and-set operation, so callers
// that use IsVisited and Visited separately race with each other: two
// goroutines can both observe a request as unvisited and fetch it twice.
// Backends are also not required to be goroutine-safe; the default one is,
// but custom implementations backed by plain maps are not, and concurrent
// map writes make Go panic. This wrapper provides the missing atomic
// operation and serializes access to the underlying backend.
type concurrentStorage struct {
	storage.Storage

	// visitedLock guards every access to the underlying visited set. It
	// covers both IsVisited and Visited so that checking and marking a
	// request as visited is a single atomic step.
	//
	// A single lock is used instead of sharding by request ID on purpose:
	// backends are not required to be goroutine-safe, so two different IDs
	// must never reach the backend at the same time.
	visitedLock sync.RWMutex

	// cookieLocks shard cookie access by host. A host's lock is held for
	// the whole read-modify-write cycle of its cookies, so concurrent
	// writers for the same host cannot lose each other's updates. Distinct
	// hosts map to distinct shards, so their merge cycles run in parallel.
	cookieLocks [cookieLockShards]sync.RWMutex

	// backendCookies guards the backend's cookie store. A backend is not
	// required to be goroutine-safe (a plain map is a typical custom
	// implementation), and Go maps may not be read and written at the same
	// time even for different keys. Reads share the lock because concurrent
	// map reads are safe; writes exclude both other writes and reads.
	backendCookies sync.RWMutex
}

// newConcurrentStorage wraps s with concurrency protection.
func newConcurrentStorage(s storage.Storage) *concurrentStorage {
	return &concurrentStorage{Storage: s}
}

// Init implements Storage.Init()
func (s *concurrentStorage) Init() error {
	return s.Storage.Init()
}

// Visited implements Storage.Visited()
func (s *concurrentStorage) Visited(requestID uint64) error {
	s.visitedLock.Lock()
	defer s.visitedLock.Unlock()
	return s.Storage.Visited(requestID)
}

// IsVisited implements Storage.IsVisited()
func (s *concurrentStorage) IsVisited(requestID uint64) (bool, error) {
	s.visitedLock.RLock()
	defer s.visitedLock.RUnlock()
	return s.Storage.IsVisited(requestID)
}

// checkAndVisit atomically reports whether requestID was visited before and,
// if it was not, marks it as visited. The returned bool is true when the
// request had already been visited, matching the semantics of IsVisited.
func (s *concurrentStorage) checkAndVisit(requestID uint64) (bool, error) {
	s.visitedLock.Lock()
	defer s.visitedLock.Unlock()
	visited, err := s.Storage.IsVisited(requestID)
	if err != nil || visited {
		return visited, err
	}
	return false, s.Storage.Visited(requestID)
}

// cookieLock returns the lock guarding cookies of the host of u.
//
// Cookies are keyed by host name, not by scheme or port, so the lock is
// derived from u.Hostname(). Cookies set over http and https for the same
// host therefore share a lock and cannot lose each other's updates.
func (s *concurrentStorage) cookieLock(u *url.URL) *sync.RWMutex {
	if u == nil {
		return &s.cookieLocks[0]
	}
	h := fnv.New32a()
	h.Write([]byte(u.Hostname()))
	return &s.cookieLocks[h.Sum32()%cookieLockShards]
}

// Cookies implements Storage.Cookies()
func (s *concurrentStorage) Cookies(u *url.URL) string {
	lock := s.cookieLock(u)
	lock.RLock()
	defer lock.RUnlock()
	s.backendCookies.RLock()
	defer s.backendCookies.RUnlock()
	return s.Storage.Cookies(u)
}

// SetCookies implements Storage.SetCookies()
func (s *concurrentStorage) SetCookies(u *url.URL, cookies string) {
	lock := s.cookieLock(u)
	lock.Lock()
	defer lock.Unlock()
	s.backendCookies.Lock()
	defer s.backendCookies.Unlock()
	s.Storage.SetCookies(u, cookies)
}

// updateCookies reads the cookies stored for u, passes them to update and
// stores the returned value. The whole cycle is atomic with respect to other
// cookie operations of the same host, so concurrent writers cannot lose each
// other's updates. The backend read and write are each protected against
// other hosts as well, as the backend itself may not be goroutine-safe.
func (s *concurrentStorage) updateCookies(u *url.URL, update func(string) string) {
	lock := s.cookieLock(u)
	lock.Lock()
	defer lock.Unlock()

	s.backendCookies.RLock()
	current := s.Storage.Cookies(u)
	s.backendCookies.RUnlock()

	s.backendCookies.Lock()
	defer s.backendCookies.Unlock()
	s.Storage.SetCookies(u, update(current))
}
