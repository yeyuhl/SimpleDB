package io.github.yeyuhl.database.concurrency;

import io.github.yeyuhl.database.TransactionContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext对LockManager进行包装以提供多粒度locking的层次结构。
 * 调用acquire/release/etc.锁，大部分应该通过LockContext来完成，它提供对层次结构中某个点(database, table X, etc.)的locking methods的访问。
 *
 * @author yeyuhl
 * @since 2023/7/20
 */
public class LockContext {

    // 底层的lock manager。
    protected final LockManager lockman;

    // 父LockContext对象，如果此LockContext位于层次结构的顶部（数据库），则为 null。
    protected final LockContext parent;

    // 此LockContext所表示的资源的名称。
    protected ResourceName name;

    // 此LockContext是否是readonly，如果是，则acquire/release/promote/escalate应该抛出UnsupportedOperationException。
    protected boolean readonly;

    // 事务编号与该事务持有的LockContext子级上的锁的数量之间的映射。
    protected final Map<Long, Integer> numChildLocks;

    // 该LockContext的子级的映射，以子级的名称为键。
    protected final Map<String, LockContext> children;

    // 任何新的子LockContext是否应标记为只读。
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name, boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * 从lock manager获取与“name”对应的lock context。
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * 获取此lock context所属的资源的名称。
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * 更新该context的childLocks的数量，该方法将递归地更新
     */
    private void updateChildLockNum(long transNum, int delta) {
        numChildLocks.putIfAbsent(transNum, 0);
        numChildLocks.put(transNum, numChildLocks.getOrDefault(transNum, 0) + delta);
        LockContext parentCTX = parentContext();
        if (parentCTX != null) {
            parentCTX.updateChildLockNum(transNum, delta);
        }
    }

    /**
     * 由于升级到SIX要释放后代上的所有S和IS锁，因此更新childLocks的数量也要更新后代context上的childLocks的数量。
     */
    private void updateChildLockNum(long transNum, int delta, List<ResourceName> names) {
        for (ResourceName name : names) {
            LockContext ctx = fromResourceName(lockman, name).parentContext();
            if (ctx != null) {
                ctx.updateChildLockNum(transNum, delta);
            }
        }
    }

    /**
     * 为事务“transaction”获取“lockType”锁。
     * 注意：必须对numChildLocks进行必要的更新，否则调用LockContext#getNumChildren将无法正常工作。
     *
     * @throws InvalidLockException          如果请求是无效的。
     * @throws DuplicateLockRequestException 如果事务已经持有锁。
     * @throws UnsupportedOperationException 如果context是只读的。
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement
        if (readonly) {
            throw new UnsupportedOperationException("the context is readonly");
        }
        // 如果parent不为空，且parent上的A锁不允许事务获取子资源上的B锁(lockType类型)，即A锁不是B锁的父锁，则抛出异常
        if (parent != null && !LockType.canBeParentLock(parent.getExplicitLockType(transaction), lockType)) {
            throw new InvalidLockException("the lock request is invalid");
        }
        lockman.acquire(transaction, getResourceName(), lockType);
        // 获取了锁后，更新numChildLocks
        LockContext parentCTX = parentContext();
        if (parentCTX != null) {
            parentCTX.updateChildLockNum(transaction.getTransNum(), 1);
        }
        return;
    }

    /**
     * 释放“transaction”对“name”所持有的锁。
     * 注意：必须对numChildLocks进行必要的更新，否则调用LockContext#getNumChildren将无法正常工作。
     *
     * @throws NoLockHeldException           如果“transaction”对“name”没有持有锁。
     * @throws InvalidLockException          如果无法释放锁，是因为这样做会违反多粒度锁定约束。
     * @throws UnsupportedOperationException 如果context是只读的。
     */
    public void release(TransactionContext transaction) throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        if (readonly) {
            throw new UnsupportedOperationException("the context is readonly");
        }
        // 如果它自身就是根节点，子节点中还有该事务所持有的锁，不能直接释放，这是违反多粒度锁定约束的
        if (getNumChildren(transaction) > 0 && parent == null) {
            throw new InvalidLockException("the lock request is invalid");
        }
        lockman.release(transaction, getResourceName());
        // 释放锁后，更新numChildLocks
        LockContext parentCTX = parentContext();
        if (parentCTX != null) {
            parentCTX.updateChildLockNum(transaction.getTransNum(), -1);
        }
        return;
    }

    /**
     * 将“transaction”的锁升级为“newLockType”。
     * 要从IS/IX升级到SIX，必须同时释放后代上的所有S和IS锁，辅助方法sisDescendants在这里可能会有所帮助。
     * 注意：必须对numChildLocks进行必要的更新，否则调用LockContext#getNumChildren将无法正常工作。
     *
     * @throws DuplicateLockRequestException 如果 `transaction` 已经持有一个 `newLockType`锁。
     * @throws NoLockHeldException           如果 `transaction` 没有锁。
     * @throws InvalidLockException          如果请求的锁类型不是升级，或者升级将导致lock manager进入无效状态（例如IS(parent), X(child)）。
     *                                       如果B可替代A并且B不等于A，或者B为SIX并且A为IS/IX/S，则从锁类型A到锁类型B的升级有效，否则无效。hasSIXAncestor在这里可能会有所帮助。
     * @throws UnsupportedOperationException 如果context是只读的。
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        if (readonly) {
            throw new UnsupportedOperationException("the context is readonly");
        }
        // 如果parent不为空，且parent上的A锁不允许事务获取子资源上的B锁(newLockType类型)，即A锁不是B锁的父锁，则抛出异常
        if (parent != null && !LockType.canBeParentLock(parent.getExplicitLockType(transaction), newLockType)) {
            throw new InvalidLockException("the lock request is invalid");
        }
        // 如果newLockType是SIX
        if (newLockType == LockType.SIX) {
            // 如果祖先中已经持有一个SIX锁
            if (hasSIXAncestor(transaction)) {
                throw new InvalidLockException("ancestor already has SIX lock, redundant lock request");
            }
            // 如果该事务没有持有任何锁，那么也无法升级
            if (lockman.getLocks(transaction).size() == 0) {
                throw new NoLockHeldException("transaction has no lock");
            }
            // 如果升级不兼容
            if (!LockType.substitutable(newLockType, lockman.getLockType(transaction, name))) {
                throw new InvalidLockException("new LockType can not substitute the old one");
            }
            List<ResourceName> sisDescendants = sisDescendants(transaction);
            updateChildLockNum(transaction.getTransNum(), -1, sisDescendants);
            sisDescendants.add(name);
            lockman.acquireAndRelease(transaction, name, newLockType, sisDescendants);
        } else {
            lockman.promote(transaction, name, newLockType);
        }
        return;
    }

    /**
     * 使用S或X锁将“事务”的锁从该context的后代升级到这一level，即从精细锁提升到粗略锁。
     * 调用之后不应有后代锁，并且在此调用之前context的后代上有效的每个操作都必须仍然有效。
     * 你应该只对lock manager进行*一次*变异调用，并且应该只从lock manager请求有关TRANSACTION的信息。
     *
     * 举个例子：如果一个事务持有：
     *
     *              IX(database)
     *              /         \
     *          IX(table1)    S(table2)
     *           /      \
     * S(table1 page3)  X(table1 page5)
     *
     * 那么在调用 table1Context.escalate(transaction) 后，我们应该有：
     *
     *         IX(database)
     *         /         \
     *    X(table1)     S(table2)
     *
     * 如果事务持有的锁未更改（例如当您连续多次调用 escalate 时），则不应进行任何变异调用。
     *
     * 注意：必须对numChildLocks进行必要的更新，否则调用LockContext#getNumChildren将无法正常工作。
     *
     * @throws NoLockHeldException           如果 `transaction` 在这个level没有锁。
     * @throws UnsupportedOperationException 如果context是只读的。
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        if (readonly) {
            throw new UnsupportedOperationException("the context is readonly");
        }
        LockType thisLevelLockType = lockman.getLockType(transaction, name);
        if (thisLevelLockType == LockType.NL) {
            throw new NoLockHeldException("transaction has no lock at this level");
        }
        List<ResourceName> descendants = getDescendants(transaction);
        boolean toX = (thisLevelLockType == LockType.IX || thisLevelLockType == LockType.SIX || thisLevelLockType == LockType.X);
        // 如果这一level的锁是S或IS
        if (!toX) {
            for (ResourceName desc : descendants) {
                LockType lt = lockman.getLockType(transaction, desc);
                // 如果后代上的锁不是S或IS，则这一level虽然是S或IS，但是为了实现对后代的读写，因此应该升级为X
                if (lt != LockType.S && lt != LockType.IS) {
                    toX = true;
                    break;
                }
            }
        }
        // 如果descendants为空且thisLevelLockType不是IS，IX，SIX这些意向锁，则不用升级
        if (descendants.isEmpty() && !thisLevelLockType.isIntent()) {
            return;
        }
        // 如果descendants不为空，意味着后代中的锁需要释放，因此需要更新numChildLocks
        if (!descendants.isEmpty()) {
            updateChildLockNum(transaction.getTransNum(), -1, descendants);
        }
        // 需要将自己加入到descendants中，因为自己当前的锁也需要释放
        descendants.add(name);
        if (toX) {
            // 如果是升级到X
            lockman.acquireAndRelease(transaction, name, LockType.X, descendants);
        } else {
            // 如果是升级到S
            lockman.acquireAndRelease(transaction, name, LockType.S, descendants);
        }
        return;
    }

    /**
     * Helper method，用于获取事务“transaction”在该context的后代上持有锁的ResourceName。
     */
    private List<ResourceName> getDescendants(TransactionContext transaction) {
        List<ResourceName> names = new ArrayList<>();
        List<Lock> locks = lockman.getLocks(transaction);
        for (Lock lock : locks) {
            if (lock.name.isDescendantOf(name))
                names.add(lock.name);
        }
        return names;
    }

    /**
     * 获取事务在此level持有的锁的类型，如果在此level没有持有锁，则获取NL。
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        return lockman.getLockType(transaction, getResourceName());
    }

    /**
     * 获取事务在此level拥有的锁的类型，可以是隐式的（例如，较高level的显式S锁意味着该level隐式拥有S锁）或显式。
     * 如果没有显式锁或隐式锁，则返回NL。
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        // 先获取显式锁
        LockType lockType = getExplicitLockType(transaction);
        if (lockType != LockType.NL) {
            return lockType;
        }
        // 如果没有显式锁，则获取隐式锁，隐式锁需要从祖先中的显式锁中间接获取
        LockContext parentCTX = parentContext();
        if (parentCTX != null) {
            LockType parentEffectiveLockType = parentCTX.getEffectiveLockType(transaction);
            // 针对于SIX，后代的隐式锁是S，而不是SIX
            if (parentEffectiveLockType == LockType.SIX) {
                lockType = LockType.S;
            } else if (!parentEffectiveLockType.isIntent()) {
                lockType = parentEffectiveLockType;
            }
        }
        return lockType;
    }

    /**
     * Helper method，用于查看事务是否在此context的祖先处持有SIX锁。
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        LockContext ancestorCTX = parentContext();
        // 遍历祖先
        while (ancestorCTX != null) {
            if (lockman.getLockType(transaction, ancestorCTX.getResourceName()) == LockType.SIX) {
                return true;
            }
            ancestorCTX = ancestorCTX.parentContext();
        }
        return false;
    }

    /**
     * Helper method，用于获取指定事务的当前context的后代中所有S或IS锁的资源名称列表。
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        List<ResourceName> names = new ArrayList<>();
        List<Lock> locks = lockman.getLocks(transaction);
        for (Lock lock : locks) {
            if (lock.name.isDescendantOf(name) && (lock.lockType == LockType.S || lock.lockType == LockType.IS)) {
                names.add(lock.name);
            }
        }
        return names;
    }

    /**
     * 禁用locking后代，这会导致该context的所有新子context都变为只读。
     * 这用于索引和临时表（临时表不允许更细粒度的锁），前者是因为locking B+树是十分复杂的事情，后者是因为临时表只能由一个事务访问，更细粒度的锁没有任何意义。
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

