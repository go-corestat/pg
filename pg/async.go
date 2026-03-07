package pg

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

type Task func(ctx context.Context, db *Client) error

type Executor struct {
	db      *Client
	jobs    chan job
	wg      sync.WaitGroup
	closed  atomic.Bool
	closeCh chan struct{}
}

type job struct {
	ctx    context.Context
	task   Task
	result chan error
}

func NewExecutor(db *Client) *Executor {
	executor := &Executor{
		db:      db,
		jobs:    make(chan job, db.config.AsyncQueueSize),
		closeCh: make(chan struct{}),
	}

	executor.wg.Add(db.config.AsyncWorkers)
	for i := 0; i < db.config.AsyncWorkers; i++ {
		go executor.worker()
	}

	return executor
}

func (e *Executor) Submit(ctx context.Context, task Task) (<-chan error, error) {
	if task == nil {
		return nil, errors.New("task is required")
	}
	if e.closed.Load() {
		return nil, errors.New("executor is closed")
	}

	result := make(chan error, 1)
	work := job{
		ctx:    ctx,
		task:   task,
		result: result,
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-e.closeCh:
		return nil, errors.New("executor is closed")
	case e.jobs <- work:
		return result, nil
	}
}

func (e *Executor) Close() {
	if !e.closed.CompareAndSwap(false, true) {
		return
	}
	close(e.closeCh)
	close(e.jobs)
	e.wg.Wait()
}

func (e *Executor) worker() {
	defer e.wg.Done()

	for work := range e.jobs {
		err := work.task(work.ctx, e.db)

		select {
		case work.result <- err:
		default:
		}
		close(work.result)
	}
}
