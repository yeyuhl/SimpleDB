package io.github.yeyuhl.database.databox.impl;

import io.github.yeyuhl.database.databox.DataBox;
import io.github.yeyuhl.database.databox.Type;
import io.github.yeyuhl.database.databox.TypeId;
/**
 * byte[]类型的DataBox的实现类
 *
 * @author yeyuhl
 * @since 2023/6/20
 */
public class ByteArrayDataBox extends DataBox {
    byte[] bytes;

    public ByteArrayDataBox(byte[] bytes, int n) {
        if (bytes.length != n) {
            throw new RuntimeException("n must be equal to the length of bytes");
        }
        this.bytes = bytes;
    }

    @Override
    public Type type() {
        return Type.byteArrayType(bytes.length);
    }

    @Override
    public TypeId getTypeId() {
        return TypeId.BYTE_ARRAY;
    }

    @Override
    public byte[] toBytes() {
        return this.bytes;
    }

    @Override
    public int compareTo(DataBox other) {
        throw new RuntimeException("Cannot compare byte arrays");
    }
    @Override
    public String toString() {
        return "byte_array";
    }
}
