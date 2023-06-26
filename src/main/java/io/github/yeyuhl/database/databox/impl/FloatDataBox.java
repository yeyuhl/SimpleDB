package io.github.yeyuhl.database.databox.impl;
import io.github.yeyuhl.database.databox.DataBox;
import io.github.yeyuhl.database.databox.Type;
import io.github.yeyuhl.database.databox.TypeId;

import java.nio.ByteBuffer;
/**
 * float类型的DataBox的实现类
 *
 * @author yeyuhl
 * @since 2023/6/20
 */
public class FloatDataBox extends DataBox {
    private float f;

    public FloatDataBox(float f) {
        this.f = f;
    }

    @Override
    public Type type() {
        return Type.floatType();
    }

    @Override
    public TypeId getTypeId() { return TypeId.FLOAT; }

    @Override
    public float getFloat() {
        return this.f;
    }

    @Override
    public byte[] toBytes() {
        return ByteBuffer.allocate(Float.BYTES).putFloat(f).array();
    }

    @Override
    public String toString() {
        return Float.toString(f);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FloatDataBox)) {
            return false;
        }
        FloatDataBox f = (FloatDataBox) o;
        return this.f == f.f;
    }

    @Override
    public int hashCode() {
        return new Float(f).hashCode();
    }

    @Override
    public int compareTo(DataBox d) {
        if (d instanceof LongDataBox) {
            long l = d.getLong();
            if (f == l) return 0;
            return f > l ? 1 : -1;
        }
        if (d instanceof IntDataBox) {
            int i = d.getInt();
            if (f == i) return 0;
            return f > i ? 1 : -1;
        }
        if (!(d instanceof FloatDataBox)) {
            String err = String.format("Invalid comparison between %s and %s.", this, d.toString());
            throw new IllegalArgumentException(err);
        }
        FloatDataBox f = (FloatDataBox) d;
        return Float.compare(this.f, f.f);
    }
}
