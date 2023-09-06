package io.github.yeyuhl.database.concurrency;

import io.github.yeyuhl.database.TransactionContext;

import java.util.*;

/**
 * LockManager维护事务对哪些资源拥有哪些锁的记录，并处理排队逻辑。
 * 通常不会直接使用lock manager，程序会调用LockContext的方法来acquire/release/promote/escalate锁。
 * <p>
 * LockManager主要关心事务、资源和锁之间的映射，并不关心多粒度，多粒度由LockContext处理。
 * <p>
 * lock manager管理的每个资源都有自己的LockRequest队列，表示在某个时间点无法满足的获取（或promote/acquire-and-release）锁的请求。
 * 每次释放资源上的锁时，应该处理该资源的队列，从第一个请求开始，按顺序处理，直到无法满足请求为止。
 * 从队列中的取出的请求应被视为该事务在没有队列的情况下释放资源后立即发出请求。
 * 举个例子：移除T1获取X(db)的请求应被视为T1刚刚请求了X(db)并且db上没有队列：
 * T1应该被赋予db上的X锁，并调用Transaction#unblock使其置于解锁状态。
 * <p>
 * 这意味在以下情况下：
 * queue: S(A) X(A) S(A)
 * 在处理队列时，只有第一个请求应该从队列中移除。
 *
 * @author yeyuhl
 * @since 2023/7/20
 */
public class LockManager {
    // transactionLocks是从事务号到该事务持有的锁对象列表的映射。
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

    // ResourceEntries是从资源名称到ResourceEntry对象的映射，其中包含对象上的锁列表以及对该资源的请求队列。
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // ResourceEntry包含资源上的锁列表以及资源上锁请求的队列。
    private class ResourceEntry {
        // 当前授予该资源的锁的列表。
        List<Lock> locks = new ArrayList<>();
        // 对该资源上尚未满足的锁请求进行排队（用Deque存储）。
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        /**
         * 检查“lockType”是否与预先存在的锁兼容。
         * 允许ID为“except”的事务所持有的锁发生冲突，这在事务尝试替换其资源上已经拥有的锁时非常有用。
         */
        public boolean checkCompatible(LockType lockType, long except) {
            // TODO(proj4_part1): implement
            for (Lock lock : locks) {
                if (lock.transactionNum == except) {
                    continue;
                }
                if (!LockType.compatible(lock.lockType, lockType)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * 为事务提供锁“lock”，假设锁是兼容的。如果事务已经拥有锁，则更新资源上的锁。
         */
        public void grantOrUpdateLock(Lock lock) {
            // TODO(proj4_part1): implement
            long transactionNum = lock.transactionNum;
            for (Lock currLock : locks) {
                if (currLock.transactionNum == transactionNum) {
                    // 如果事务已经拥有锁，则更新lock
                    currLock.lockType = lock.lockType;
                    updateLockToTransaction(lock);
                    return;
                }
            }
            // 如果事务没有锁，则为事务提供lock
            locks.add(lock);
            addLockToTransaction(lock);
            return;
        }

        /**
         * 释放锁“lock”并处理队列，假设之前已授予锁。
         */
        public void releaseLock(Lock lock) {
            // TODO(proj4_part1): implement
            locks.remove(lock);
            removeLockFromTransaction(lock);
            processQueue();
            return;
        }

        /**
         * 如果addFront为true，则将“request”添加到队列的前面，否则添加到末尾。
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            // TODO(proj4_part1): implement
            if (addFront) {
                waitingQueue.addFirst(request);
            } else {
                waitingQueue.addLast(request);
            }
            return;
        }

        /**
         * 从队列的前端到后端向请求授予lock，当无法授予下一个lock时停止。
         * 一旦请求被完全批准，发出请求的事务就可以被解锁。
         */
        private void processQueue() {
            Iterator<LockRequest> requests = waitingQueue.iterator();
            // TODO(proj4_part1): implement
            LockRequest request;
            while (requests.hasNext()) {
                request = requests.next();
                if (checkCompatible(request.lock.lockType, request.transaction.getTransNum())) {
                    // 从队列中删除请求
                    waitingQueue.removeFirst();
                    // 如果请求是兼容的，则授予锁
                    grantOrUpdateLock(request.lock);
                    // 释放锁
                    for (Lock lock : request.releasedLocks) {
                        release(request.transaction, lock.name);
                    }
                    // 释放事务
                    request.transaction.unblock();
                } else {
                    // 如果请求不兼容，则停止处理队列
                    break;
                }
            }
            return;
        }

        /**
         * 获取“transaction”对此资源所上的锁的类型。
         */
        public LockType getTransactionLockType(long transaction) {
            // TODO(proj4_part1): implement
            for (Lock lock : locks) {
                if (lock.transactionNum == transaction) {
                    return lock.lockType;
                }
            }
            return LockType.NL;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                    ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<String, LockContext> contexts = new HashMap<>();

    /**
     * Helper method，用于获取与“name”对应的resourceEntry。
     * 如果尚不存在该条目，则将新的（空）resourceEntry插入到映射中。
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    /**
     * Helper method，向指定事务更新lock，并更新transactionLocks数据结构。
     */
    private void updateLockToTransaction(Lock lock) {
        long transactionNum = lock.transactionNum;
        List<Lock> locks = transactionLocks.get(transactionNum);
        for (Lock exist : locks) {
            if (exist.name.equals(lock.name)) {
                exist.lockType = lock.lockType;
            }
        }
    }

    /**
     * Helper method，向指定事务移除lock，并更新transactionLocks数据结构。
     */
    private void removeLockFromTransaction(Lock lock) {
        long transactionNum = lock.transactionNum;
        transactionLocks.get(transactionNum).remove(lock);
    }

    /**
     * Helper method，向指定事务添加lock，并更新transactionLocks数据结构。
     */
    private void addLockToTransaction(Lock lock) {
        long transactionNum = lock.transactionNum;
        transactionLocks.putIfAbsent(transactionNum, new ArrayList<>());
        transactionLocks.get(transactionNum).add(lock);
    }

    /**
     * 为事务“transaction”获取“name”上的“lockType”锁，并在一次原子操作中获取该锁后释放该事务持有的“releaseNames”上的所有锁。
     *
     * 在获取或释放任何锁之前必须进行错误检查，如果新锁与资源上的另一个事务的锁不兼容，则该事务将被阻止，且其请求会被放置在资源的队列的前面。
     *
     * 仅在获取所请求的锁后才释放“releaseNames”上的锁，并且应该处理相应的队列。
     *
     * 在执行获取和释放的操作时，释放“name”上的旧锁的不应更改“name”上锁的获取时间。
     * 举个例子：如果事务按照以下顺序获取锁：S(A)、X(B)、获取X(A)和释放S(A)，则认为A上的锁在B上的锁之前已获取。
     *
     * @throws DuplicateLockRequestException 如果“name”上的锁已被“transaction”持有并且未被释放
     * @throws NoLockHeldException           如果“transaction”没有持有“releaseNames”中的一个或多个名称的锁
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name, LockType lockType, List<ResourceName> releaseNames)
            throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(proj4_part1): implement
        boolean shouldBlock = false;
        long transactionNum = transaction.getTransNum();
        synchronized (this) {
            ResourceEntry resourceEntry = getResourceEntry(name);
            // 检查是否已经持有锁
            if (resourceEntry.getTransactionLockType(transactionNum) == lockType) {
                throw new DuplicateLockRequestException("Transaction " + transactionNum + " already holds a lock on " + name);
            }
            Lock lock = new Lock(name, lockType, transactionNum);
            // 检查是否兼容，如果不兼容则阻塞事务
            if (!resourceEntry.checkCompatible(lockType, transactionNum)) {
                shouldBlock = true;
                List<Lock> releasedLocks = new ArrayList<>();
                for (ResourceName releaseName : releaseNames) {
                    LockType locktype = getResourceEntry(releaseName).getTransactionLockType(transactionNum);
                    releasedLocks.add(new Lock(name, locktype, transactionNum));
                }
                // 建立要释放的不兼容的锁的请求并把请求放到队列的前面
                LockRequest request = new LockRequest(transaction, lock, releasedLocks);
                resourceEntry.addToQueue(request, true);
                transaction.prepareBlock();
            } else {
                // 如果兼容则先获取锁，然后释放“releaseNames”上的锁
                resourceEntry.grantOrUpdateLock(lock);
                for (ResourceName releaseName : releaseNames) {
                    // 如果releaseName和name相同则跳过
                    if (releaseName.equals(name)) {
                        continue;
                    }
                    // error checking
                    if (getResourceEntry(releaseName).getTransactionLockType(transactionNum) == LockType.NL) {
                        throw new NoLockHeldException("Transaction " + transactionNum + " does not hold a lock on " + releaseName);
                    }
                    release(transaction, releaseName);
                }
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * 为事务“transaction”获取“name”上的“lockType”锁。
     *
     * 获取锁之前必须进行错误检查,如果新锁与该资源上的另一个事务的锁不兼容，或者如果该资源的队列中有其他事务，则该事务将被阻止，并且该请求将被放置在资源名为NAME的队列的后面。
     *
     * @throws DuplicateLockRequestException 如果“transaction”持有对“name”的lock
     */
    public void acquire(TransactionContext transaction, ResourceName name, LockType lockType) throws DuplicateLockRequestException {
        // TODO(proj4_part1): implement
        boolean shouldBlock = false;
        long transactionNum = transaction.getTransNum();
        synchronized (this) {
            ResourceEntry resourceEntry = getResourceEntry(name);
            if (resourceEntry.getTransactionLockType(transactionNum) != LockType.NL) {
                throw new DuplicateLockRequestException("Transaction " + transactionNum + " already holds a lock on " + name);
            }
            Lock lock = new Lock(name, lockType, transactionNum);
            // 进行错误检查，检查是否兼容或者该资源的队列中是否有其他事务
            if (!resourceEntry.checkCompatible(lockType, transactionNum) || !resourceEntry.waitingQueue.isEmpty()) {
                shouldBlock = true;
                // 生成请求并放到队列的后面
                LockRequest request = new LockRequest(transaction, lock);
                resourceEntry.addToQueue(request, false);
                transaction.prepareBlock();
            } else {
                // 错误检查通过则直接获取锁
                resourceEntry.grantOrUpdateLock(lock);
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * 释放“transaction”对“name”上的lock，释放锁之前必须进行错误检查。
     *
     * 资源名称的队列应在此调用后处理，如果队列中的任何请求有需要释放的锁，则应释放这些锁，并处理相应的队列。
     *
     * @throws NoLockHeldException 如果“transaction”对“name”没有持有锁
     */
    public void release(TransactionContext transaction, ResourceName name) throws NoLockHeldException {
        // TODO(proj4_part1): implement
        long transactionNum = transaction.getTransNum();
        synchronized (this) {
            ResourceEntry resourceEntry = getResourceEntry(name);
            LockType lockType = resourceEntry.getTransactionLockType(transactionNum);
            // 检查是否持有锁
            if (lockType == LockType.NL) {
                throw new NoLockHeldException("Transaction " + transactionNum + " does not hold a lock on " + name);
            }
            Lock lock = new Lock(name, lockType, transactionNum);
            // 释放锁
            resourceEntry.releaseLock(lock);
            // 处理队列
            resourceEntry.processQueue();
        }
    }

    /**
     * 将事务对“name”上持有的lock升级为“newLockType”。
     * 举个例子：将事务对“name”上持有的lock从当前lock type更改为“newLockType”（如果这是有效的替换）。
     *
     * 在更改任何锁之前必须进行错误检查，如果新锁与资源上的另一个事务的锁不兼容，则该事务将被阻止，并且请求将被放置在资源队列的前面。
     *
     * 锁的升级不应该改变锁的获取时间，即如果事务按照以下顺序获取锁：S(A)、X(B)、promoteX(A)，则A上的锁被认为是在B上的锁之前获取的。
     *
     * @throws DuplicateLockRequestException 如果“transaction”已经在“name”上有一个“newLockType”的锁
     * @throws NoLockHeldException           如果“transaction”在“name”上没有锁
     * @throws InvalidLockException          如果请求的锁定类型不是promotion(向上升级)，当且仅当B可替代A，且B不等于A时，从锁类型A到锁类型B的升级才有效。
     */
    public void promote(TransactionContext transaction, ResourceName name, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part1): implement
        boolean shouldBlock = false;
        long transNum = transaction.getTransNum();
        synchronized (this) {
            ResourceEntry entry = getResourceEntry(name);
            if (entry.getTransactionLockType(transNum) == newLockType) {
                throw new DuplicateLockRequestException("transaction " + transNum + " already has a " + newLockType + " " + name);
            }
            if (entry.getTransactionLockType(transNum) == LockType.NL) {
                throw new NoLockHeldException("transaction " + transNum + " has no lock on " + name);
            }
            if (!LockType.substitutable(newLockType, entry.getTransactionLockType(transNum))) {
                throw new InvalidLockException("the new lock type is not substitutable for the old lock");
            }
            Lock lock = new Lock(name, newLockType, transNum);
            if (!entry.checkCompatible(newLockType, transNum)) {
                shouldBlock = true;
                LockRequest request = new LockRequest(transaction, lock);
                entry.addToQueue(request, true);
                transaction.prepareBlock();
            } else {
                entry.grantOrUpdateLock(lock);
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * 返回“transaction”对“name”上的锁的类型，如果没有持有锁，则返回NL。
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // TODO(proj4_part1): implement
        ResourceEntry resourceEntry = getResourceEntry(name);
        long transactionNum = transaction.getTransNum();
        return resourceEntry.getTransactionLockType(transactionNum);

    }

    /**
     * 按获取顺序返回“name”上持有的锁的列表。
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * 按获取顺序返回“transaction”持有的锁的列表。
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(), Collections.emptyList()));
    }

    /**
     * 创建LockContext。有关详细信息，请参阅此文件顶部和 LockContext.java 顶部的注释。
     */
    public synchronized LockContext context(String name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, name));
        }
        return contexts.get(name);
    }

    /**
     * 为数据库创建LockContext。有关详细信息，请参阅此文件顶部和 LockContext.java 顶部的注释。
     */
    public synchronized LockContext databaseContext() {
        return context("database");
    }
}
