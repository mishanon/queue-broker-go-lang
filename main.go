package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type queue struct {
	mu      sync.Mutex
	items   []string
	waiters []chan string
}

type broker struct {
	mu     sync.RWMutex
	queues map[string]*queue
}

func newBroker() *broker {
	return &broker{queues: make(map[string]*queue)}
}

func (b *broker) getQueue(name string) *queue {
	b.mu.RLock()
	q, ok := b.queues[name]
	b.mu.RUnlock()
	if ok {
		return q
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	q, ok = b.queues[name]
	if ok {
		return q
	}
	q = &queue{}
	b.queues[name] = q
	return q
}

func (q *queue) put(msg string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.waiters) > 0 {
		waiter := q.waiters[0]
		q.waiters = q.waiters[1:]
		waiter <- msg
		return
	}
	q.items = append(q.items, msg)
}

func (q *queue) get(timeoutSec int) (string, bool) {
	q.mu.Lock()
	if len(q.items) > 0 {
		msg := q.items[0]
		q.items = q.items[1:]
		q.mu.Unlock()
		return msg, true
	}

	if timeoutSec == 0 {
		q.mu.Unlock()
		return "", false
	}

	ch := make(chan string)
	q.waiters = append(q.waiters, ch)
	q.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
	defer cancel()

	select {
	case msg := <-ch:
		return msg, true
	case <-ctx.Done():
		q.mu.Lock()
		for i, w := range q.waiters {
			if w == ch {
				q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
				break
			}
		}
		q.mu.Unlock()
		return "", false
	}
}

func main() {
	port := flag.String("port", "8080", "port to listen on")
	flag.Parse()

	b := newBroker()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		queueName := r.URL.Path[1:]
		if queueName == "" {
			http.NotFound(w, r)
			return
		}

		switch r.Method {
		case http.MethodPut:
			msg := r.URL.Query().Get("v")
			if msg == "" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			b.getQueue(queueName).put(msg)
			w.WriteHeader(http.StatusOK)

		case http.MethodGet:
			timeoutStr := r.URL.Query().Get("timeout")
			timeout := 0
			if timeoutStr != "" {
				var err error
				timeout, err = strconv.Atoi(timeoutStr)
				if err != nil {
					timeout = 0
				}
			}
			msg, ok := b.getQueue(queueName).get(timeout)
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Write([]byte(msg))

		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	addr := ":" + *port
	fmt.Printf("Starting server on %s\n", addr)
	http.ListenAndServe(addr, nil)
}
