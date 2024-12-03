// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecstore

import (
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type partitionLock struct {
	mu struct {
		syncutil.RWMutex
		reentrancy int
	}
	exclusiveTxnId atomic.Uint64
}

func (pl *partitionLock) IsAcquiredBy(id txnId) bool {
	return txnId(pl.exclusiveTxnId.Load()) == id
}

func (pl *partitionLock) Acquire(id txnId) {
	if txnId(pl.exclusiveTxnId.Load()) == id {
		// Exclusive lock has already been acquired by the transaction.
		pl.mu.reentrancy++
		return
	}

	// Block until exclusive lock is acquired.
	pl.mu.Lock()
	pl.exclusiveTxnId.Store(uint64(id))
}

func (pl *partitionLock) AcquireShared(id txnId) {
	if id != 0 && txnId(pl.exclusiveTxnId.Load()) == id {
		// Exclusive lock has already been acquired by the transaction.
		pl.mu.reentrancy++
		return
	}

	// Block until shared lock is acquired.
	pl.mu.RLock()
}

func (pl *partitionLock) TryAcquire(id txnId) bool {
	if txnId(pl.exclusiveTxnId.Load()) == id {
		// Exclusive lock has already been acquired by the transaction.
		pl.mu.reentrancy++
		return true
	}

	// Block until exclusive lock is acquired.
	if !pl.mu.TryLock() {
		return false
	}
	pl.exclusiveTxnId.Store(uint64(id))
	return true
}

func (pl *partitionLock) Release() {
	if pl.mu.reentrancy > 0 {
		pl.mu.reentrancy--
		return
	}

	// No remaining re-entrancy, so release lock.
	pl.exclusiveTxnId.Store(0)
	pl.mu.Unlock()
}

func (pl *partitionLock) ReleaseShared() {
	if pl.mu.reentrancy > 0 {
		pl.mu.reentrancy--
		return
	}

	// No remaining reentrancy, so release lock.
	pl.mu.RUnlock()
}
