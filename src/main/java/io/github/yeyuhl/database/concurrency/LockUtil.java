package io.github.yeyuhl.database.concurrency;

import io.github.yeyuhl.database.TransactionContext;

/**
 * LockUtil是一个声明层，它简化了用户多粒度锁获取步骤。
 * 一般来说，应该使用LockUtil来获取锁，而不是直接调用LockContext方法。
 *
 * @author yeyuhl
 * @since 2023/7/23
 */
public class LockUtil {
    /**
     * 确保当前事务可以在“lockContext”上执行请求“requestType”的操作，`requestType`应为以下之一：S、X、NL。
     * 此方法应根据需求进行promote/escalate/acquire，但应仅授予所需的最少许可集的锁。
     * 考虑以下情况：
     * - 当前锁的类型可以有效地替代请求的类型
     * - 当前锁的类型为IX，请求的锁为S
     * - 当前锁的类型是意向锁
     * - 以上都不是，在这种情况下，请考虑显式锁的类型可以是什么值，并考虑祖先是否需要acquired和changed。
     * 你可能会发现需要创建一个helper method来确保所有祖先能拥有适当的锁。
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType必须是S,X或NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // 如果transaction或lockContext是null，则什么都不做
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null | lockContext == null) return;

        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement
        // case1：当前锁的类型可以有效地替代请求的类型
        if (LockType.substitutable(effectiveLockType, requestType)) {
            return;
        }
        // case2：当前锁的类型为IX，请求的锁为S
        if (explicitLockType == LockType.IX && requestType == LockType.S) {
            // 将原有的锁升级为SIX
            lockContext.promote(transaction, LockType.SIX);
            return;
        }
        // case3：当前锁的类型是意向锁
        if (explicitLockType.isIntent()) {
            // 由于是意向锁，所以升级为粗略锁
            lockContext.escalate(transaction);
            explicitLockType = lockContext.getExplicitLockType(transaction);
            if (explicitLockType == requestType || explicitLockType == LockType.X) {
                return;
            }
        }
        // case4：以上都不满足，在这种情况下，考虑显式锁的类型可以是什么值，并考虑祖先是否需要acquired和changed。
        // 更具体地说，（显式锁类型，请求类型）对，只能是（NL，S），（NL，X），（S，X）
        // 如果请求的是S，那么其祖先应为IS，如果请求的是X，那么其祖先应为IX
        if (requestType == LockType.S) {
            enforceLock(transaction, parentContext, LockType.IS);
        } else {
            enforceLock(transaction, parentContext, LockType.IX);
        }
        // 如果当前锁的类型是NL，则获取，否则升级到请求的锁类型
        if (explicitLockType == LockType.NL) {
            lockContext.acquire(transaction, requestType);
        } else {
            lockContext.promote(transaction, requestType);
        }
        return;
    }

    /**
     * helper method，确保某一节点之前的祖先都能获取到相应的锁
     */
    private static void enforceLock(TransactionContext transaction, LockContext lockContext, LockType lockType) {
        assert (lockType == LockType.IS || lockType == LockType.IX);
        if (lockContext == null) {
            return;
        }
        // 递归调用，直到lockContext为null才返回，确保了所有祖先都能遍历到
        enforceLock(transaction, lockContext.parentContext(), lockType);
        // 获取当前事务在lockContext上的锁类型
        LockType currLockType = lockContext.getExplicitLockType(transaction);
        // 如果currLockType不能替代lockType
        if (!LockType.substitutable(currLockType, lockType)) {
            // 空则获取，否则升级
            if (currLockType == LockType.NL) {
                lockContext.acquire(transaction, lockType);
            } else {
                lockContext.promote(transaction, lockType);
            }
        }
    }

}
