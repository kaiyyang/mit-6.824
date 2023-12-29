package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

func (entry LogEntry) String() string {
	return fmt.Sprintf("{Index:%v,Term:%v}", entry.Index, entry.Term)
}

type lockedrand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedrand) Intn(n int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rand.Intn(n)
}

var globalRand = &lockedrand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

const (
	HeartbeatTimeout = 125
	ElectionTimeout  = 1000
)

func StableHeartbeatTimeout() time.Duration {
	return time.Duration(HeartbeatTimeout) * time.Millisecond
}

func RandomizedElectionTimeout() time.Duration {
	return time.Duration(ElectionTimeout+globalRand.Intn(ElectionTimeout)) * time.Millisecond
}

func Min(x, y int) int {
	if x > y {
		return y
	}
	return x
}

func Max(x, y int) int {
	if x < y {
		return y
	}
	return x
}

func insertionSort(sl []int) {
	a, b := 0, len(sl)
	for i := a + 1; i < b; i++ {
		for j := i; j > a && sl[j] < sl[j-1]; j-- {
			sl[j], sl[j-1] = sl[j-1], sl[j]
		}
	}
}

func shrinkEntriesArray(entries []LogEntry) []LogEntry {
	const lenMultiple = 2
	if len(entries)*lenMultiple < cap(entries) {
		newEntries := make([]LogEntry, len(entries))
		copy(newEntries, entries)
		return newEntries
	}
	return entries
}

// Helpers
func (rf *Raft) getFirstLog() LogEntry {
	return rf.logs[0]
}

func (rf *Raft) getLastLog() LogEntry {
	return rf.logs[len(rf.logs)-1]
}
