package io.github.yeyuhl.database.concurrency;

/**
 * LockType提供跟踪不同锁类型之间关系的实用方法。
 *
 * @author yeyuhl
 * @since 2023/7/23
 */
public enum LockType {
    S,   // 共享锁
    X,   // 独占锁（排他锁）
    IS,  // 意图共享
    IX,  // 意图独占
    SIX, // 共享意图独占
    NL;  // 没有持有锁

    /**
     * 此方法检查锁类型A和B是否彼此兼容。
     * 如果一个事务可以在某个资源上持有类型A的锁，同时另一个事务在同一资源上持有类型B的锁，则这些锁类型是兼容的。
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
            case NL:
                return true;
            // IS和X不兼容
            case IS:
                return !(b == X);
            // IX和NL、IS、IX兼容
            case IX:
                return (b == NL) || (b == IS) || (b == IX);
            //  S和NL、IS、S兼容
            case S:
                return (b == NL) || (b == IS) || (b == S);
            case SIX:
                return (b == NL) || (b == IS);
            // X和NL兼容
            case X:
                return (b == NL);
            default:
                throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
            case S:
                return IS;
            case X:
                return IX;
            case IS:
                return IS;
            case IX:
                return IX;
            case SIX:
                return IX;
            case NL:
                return NL;
            default:
                throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * 判断parentLockType是否有权向子级授予childLockType。
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        switch (parentLockType) {
            case NL:
                return childLockType == NL;
            case IS:
                return childLockType == NL || childLockType == IS || childLockType == S;
            case IX:
                return true;
            case S:
                return childLockType == NL;
            case SIX:
                return childLockType == NL || childLockType == X || childLockType == IX || childLockType == SIX;
            case X:
                return childLockType == NL;
            default:
                throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * 判断一个锁是否可以用于替代另一个锁（例如，S锁可以用X锁代替），substitute替代required。
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        switch (substitute) {
            case NL:
                return required == NL;
            case IS:
                return required == NL || required == IS;
            case IX:
                return required == NL || required == IS || required == IX;
            case S:
                return required == NL || required == IS || required == S;
            case SIX:
                return !(required == X);
            case X:
                return true;
            default:
                throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
            case S:
                return "S";
            case X:
                return "X";
            case IS:
                return "IS";
            case IX:
                return "IX";
            case SIX:
                return "SIX";
            case NL:
                return "NL";
            default:
                throw new UnsupportedOperationException("bad lock type");
        }
    }
}

