package main

import (
	"fmt"
	"sync"
	//"time"
	//"math"
)

type SafeMap struct {
	freqMap map[string]int
	m       *sync.RWMutex
}

type Job struct {
	Id int
	Wg *sync.WaitGroup
}

type Worker struct {
	ID          int
	Work        chan string
	WorkerQueue chan chan string
	QuitChan    chan bool
}

func (w *Worker) Start() {
	go func() {
		for {
			// Add ourselves into the worker queue.
			w.WorkerQueue <- w.Work

			select {
			case work := <-w.Work: // Leader sent a work request
				fmt.Printf("worker %d received work request", w.ID)

				if work == "map" {
					w.Work <- "ok map" // send confirmation of "map" keyword to leader
				}

				chunk := w.Work
				results := countWords(chunk)
				w.Work <- results

			case <-w.QuitChan: // no more chunks
				// TODO: all workers stop
				fmt.Printf("worker%d stopping\n", w.ID)
				return
			}
		}
	}()
}

func countWords(chunk chan string) *SafeMap {
	// TODO
	return &SafeMap{}
}
