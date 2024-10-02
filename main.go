package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Task struct {
	ID      int
	Execute func()
}

type Worker struct {
	taskQueue chan Task
	stopChan  chan bool
}

type ThreadPool struct {
	mu              sync.Mutex
	workers         []*Worker
	minSize         int
	maxSize         int
	idleTime        time.Duration
	maxTimeout      time.Duration
	taskQueue       chan Task
	workerAvailable chan *Worker
	wg              sync.WaitGroup
	ctx             context.Context
	cancel          context.CancelFunc
}

func NewThreadPool(minSize, maxSize int, idleTime, maxTimeout time.Duration) *ThreadPool {
	ctx, cancel := context.WithCancel(context.Background())
	tp := &ThreadPool{
		minSize:         minSize,
		maxSize:         maxSize,
		idleTime:        idleTime,
		maxTimeout:      maxTimeout,
		taskQueue:       make(chan Task),             // unbuffered channel, send/recieve will block until other side is ready
		workerAvailable: make(chan *Worker, maxSize), // buffered channel
		ctx:             ctx,
		cancel:          cancel,
	}

	// Initialize the pool with minSize workers
	fmt.Println("Adding min amout of workers on init")
	for i := 0; i < minSize; i++ {
		tp.addWorker()
	}

	return tp
}

func (tp *ThreadPool) addWorker() {
	// fmt.Println("Adding worker")

	tp.mu.Lock()
	defer tp.mu.Unlock()

	if (len(tp.workers) >= tp.maxSize) || (len(tp.workerAvailable) == 0) {
		// fmt.Println("Workers length is greater than or equal to max, not creating more workers")
		return
	}

	worker := &Worker{
		taskQueue: make(chan Task),
		stopChan:  make(chan bool),
	}
	tp.workers = append(tp.workers, worker)
	tp.workerAvailable <- worker

	// fmt.Println("Starting worker loop")
	go tp.workerLoop(worker)
}

func (tp *ThreadPool) workerLoop(worker *Worker) {
	defer tp.wg.Done()

	idleTimer := time.NewTimer(tp.idleTime)
	defer idleTimer.Stop()

	for {
		select {
		case task := <-worker.taskQueue:
			// Execute the task
			// fmt.Printf("Worker processing task %d\n", task.ID)
			task.Execute()               // Execute the task's function
			idleTimer.Reset(tp.idleTime) // Reset the idle timer on task processing

		case <-idleTimer.C:
			// If idle for too long, remove worker
			fmt.Println("Worker idle, trying to remove from the pool")
			tp.removeWorker(worker)
			return

		case <-worker.stopChan:
			// If stop is requested
			fmt.Println("Stopping worker")
			return

		case <-tp.ctx.Done():
			// Pool shutdown
			fmt.Println("Shutting down worker")
			return
		}
	}
}

func (tp *ThreadPool) removeWorker(worker *Worker) {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	if len(tp.workers) <= tp.minSize {
		return
	}

	// Remove worker from pool
	for i, w := range tp.workers {
		if w == worker {
			tp.workers = append(tp.workers[:i], tp.workers[i+1:]...)
			close(w.stopChan)
			return
		}
	}
}

func (tp *ThreadPool) Submit(task Task) error {
	fmt.Println("Submitting task", task.ID)

	tp.wg.Add(1)

	fmt.Println("Workers available", len(tp.workerAvailable))
	tp.addWorker()
	fmt.Println("Workers length", len(tp.workers))

	select {
	case worker := <-tp.workerAvailable:
		// A worker is available, assign the task
		worker.taskQueue <- task
		tp.workerAvailable <- worker

	case <-time.After(tp.maxTimeout):
		// Timeout: no workers are available
		tp.wg.Done()
		return errors.New("timeout: no available workers")
	}

	return nil
}

// Shutdown gracefully shuts down the thread pool
func (tp *ThreadPool) Shutdown() {
	// Cancel the context to stop the pool
	tp.cancel()

	// Wait for all workers to finish processing
	tp.wg.Wait()
}

func main() {
	threadpool := NewThreadPool(3, 10, time.Second*10, time.Second*30)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		fmt.Println("\nReceived interrupt signal. Cleaning up...")
		threadpool.Shutdown()

		os.Exit(0)
	}()

	threadpool.Submit(Task{ID: 1, Execute: func() {
		time.Sleep(time.Second * 3)
		fmt.Println("Excuting taskid 1 callback")
	}})
	threadpool.Submit(Task{ID: 2, Execute: func() {
		time.Sleep(time.Second * 3)
		fmt.Println("Excuting taskid 2 callback")
	}})
	threadpool.Submit(Task{ID: 3, Execute: func() {
		time.Sleep(time.Second * 3)
		fmt.Println("Excuting taskid 3 callback")
	}})
	threadpool.Submit(Task{ID: 4, Execute: func() {
		time.Sleep(time.Second * 3)
		fmt.Println("Excuting taskid 4 callback")
	}})
	threadpool.Submit(Task{ID: 5, Execute: func() {
		time.Sleep(time.Second * 3)
		fmt.Println("Excuting taskid 5 callback")
	}})
	threadpool.Submit(Task{ID: 6, Execute: func() {
		time.Sleep(time.Second * 3)
		fmt.Println("Excuting taskid 6 callback")
	}})
	threadpool.Submit(Task{ID: 7, Execute: func() {
		time.Sleep(time.Second * 3)
		fmt.Println("Excuting taskid 7 callback")
	}})
	threadpool.Submit(Task{ID: 8, Execute: func() {
		time.Sleep(time.Second * 3)
		fmt.Println("Excuting taskid 8 callback")
	}})
	threadpool.Submit(Task{ID: 9, Execute: func() {
		time.Sleep(time.Second * 3)
		fmt.Println("Excuting taskid 9 callback")
	}})
	threadpool.Submit(Task{ID: 10, Execute: func() {
		time.Sleep(time.Second * 3)
		fmt.Println("Excuting taskid 10 callback")
	}})
	threadpool.Submit(Task{ID: 11, Execute: func() {
		time.Sleep(time.Second * 3)
		fmt.Println("Excuting taskid 11 callback")
	}})
	threadpool.Submit(Task{ID: 12, Execute: func() {
		time.Sleep(time.Second * 3)
		fmt.Println("Excuting taskid 12 callback")
	}})
	threadpool.Submit(Task{ID: 13, Execute: func() {
		time.Sleep(time.Second * 3)
		fmt.Println("Excuting taskid 13 callback")
	}})
	threadpool.Submit(Task{ID: 14, Execute: func() {
		time.Sleep(time.Second * 3)
		fmt.Println("Excuting taskid 14 callback")
	}})
	threadpool.Submit(Task{ID: 15, Execute: func() {
		time.Sleep(time.Second * 3)
		fmt.Println("Excuting taskid 15 callback")
	}})
}
