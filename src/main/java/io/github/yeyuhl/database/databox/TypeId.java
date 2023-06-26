package io.github.yeyuhl.database.databox;

/**
 * 类型的枚举类
 *
 * @author yeyuhl
 * @since 2023/6/20
 */
public enum TypeId {
    BOOL,
    INT,
    FLOAT,
    STRING,
    LONG,
    BYTE_ARRAY;

    private static final TypeId[] values = TypeId.values();

    /**
     * 通过序列号获取TypeId的值
     */
    public static TypeId fromInt(int x) {
        if (x < 0 || x >= values.length) {
            String err = String.format("Unknown TypeId ordinal %d.", x);
            throw new IllegalArgumentException(err);
        }
        return values[x];
    }
}
