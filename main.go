package main

import (
	"flag"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

/*
#include "db.h"
*/
// #cgo CXXFLAGS: -std=c++11
import "C"

var numWorkers = flag.Int("w", 100, "")
var groupType = flag.String("t", "go", "")
var numOps uint64

type db struct {
	groupInGo  bool
	cdb        *C.DB
	mu         sync.Mutex
	cond       *sync.Cond
	committing bool
	commitSeq  uint64
	pendingSeq uint64
	pending    []*batch
}

func newDB(groupInGo bool) *db {
	db := &db{
		groupInGo: groupInGo,
		cdb:       C.NewDB(),
	}
	db.cond = sync.NewCond(&db.mu)
	return db
}

func (db *db) newBatch() *batch {
	return &batch{
		cbatch: C.NewBatch(db.cdb),
	}
}

func (db *db) commit(b *batch) int {
	if !db.groupInGo {
		return int(C.CommitBatch(db.cdb, b.cbatch))
	}

	db.mu.Lock()
	leader := len(db.pending) == 0
	seq := db.pendingSeq
	db.pending = append(db.pending, b)
	var size int

	if leader {
		// We're the leader. Wait for any running commit to finish.
		for db.committing {
			db.cond.Wait()
		}
		pending := db.pending
		db.pending = nil
		db.pendingSeq++
		db.committing = true
		db.mu.Unlock()

		for _, p := range pending[1:] {
			b.combine(p)
		}
		b.apply()
		size = len(pending)

		db.mu.Lock()
		db.committing = false
		db.commitSeq = seq
		db.cond.Broadcast()
	} else {
		// We're a follower. Wait for the commit to finish.
		for db.commitSeq < seq {
			db.cond.Wait()
		}
	}
	db.mu.Unlock()
	return size
}

type batch struct {
	cbatch *C.Batch
}

func (b *batch) combine(other *batch) {
	// In the real implementation, this combines the batches together.
}

func (b *batch) apply() {
	C.ApplyBatch(b.cbatch)
}

func (b *batch) free() {
	C.FreeBatch(b.cbatch)
}

func worker(db *db) {
	for {
		b := db.newBatch()
		db.commit(b)
		b.free()

		atomic.AddUint64(&numOps, 1)
		time.Sleep(time.Microsecond)
	}
}

func main() {
	flag.Parse()

	var groupInGo bool
	switch *groupType {
	case "go":
		groupInGo = true
	case "cgo":
		groupInGo = false
	default:
		log.Fatalf("unknown batch type: %s", *groupType)
	}

	db := newDB(groupInGo)
	for i := 0; i < *numWorkers; i++ {
		go worker(db)
	}

	start := time.Now()
	lastNow := start
	var lastOps uint64

	for i := 0; ; i++ {
		time.Sleep(time.Second)
		if i%20 == 0 {
			fmt.Println("_elapsed____ops/sec")
		}

		now := time.Now()
		elapsed := now.Sub(lastNow)
		ops := atomic.LoadUint64(&numOps)

		fmt.Printf("%8s %10.1f\n",
			time.Duration(time.Since(start).Seconds()+0.5)*time.Second,
			float64(ops-lastOps)/elapsed.Seconds())

		lastNow = now
		lastOps = ops
	}
}
