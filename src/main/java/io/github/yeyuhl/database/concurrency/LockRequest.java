package io.github.yeyuhl.database.concurrency;

import io.github.yeyuhl.database.TransactionContext;

import java.util.Collections;
import java.util.List;

/**
 * Represents a lock request on the queue for `transaction`, requesting `lock`
 * and releasing everything in `releasedLocks`. `lock` should be granted and
 * everything in `releasedLocks` should be released *before* the transaction is
 * unblocked.
 */
class LockRequest {
    TransactionContext transaction;
    Lock lock;
    List<Lock> releasedLocks;

    // Lock request for `lock`, that is not releasing anything.
    LockRequest(TransactionContext transaction, Lock lock) {
        this.transaction = transaction;
        this.lock = lock;
        this.releasedLocks = Collections.emptyList();
    }

    // Lock request for `lock`, in exchange for all the locks in `releasedLocks`.
    LockRequest(TransactionContext transaction, Lock lock, List<Lock> releasedLocks) {
        this.transaction = transaction;
        this.lock = lock;
        this.releasedLocks = releasedLocks;
    }

    @Override
    public String toString() {
        return "Request for " + lock.toString() + " (releasing " + releasedLocks.toString() + ")";
    }
}