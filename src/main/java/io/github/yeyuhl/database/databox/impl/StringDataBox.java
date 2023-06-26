package io.github.yeyuhl.database.databox.impl;

import io.github.yeyuhl.database.databox.DataBox;
import io.github.yeyuhl.database.databox.Type;
import io.github.yeyuhl.database.databox.TypeId;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * String类型的DataBox的实现类
 *
 * @author yeyuhl
 * @since 2023/6/20
 */
public class StringDataBox extends DataBox {
    private String s;
    private int m;

    /**
     * 构建一个长度为length字节的StringDataBox
     * 如果length小于0则抛出异常，如果value的长度大于length则截断value，如果value的长度小于length则在value用空字节填充
     */
    public StringDataBox(String s, int m) {
        if (m <= 0) {
            String msg = String.format("Cannot construct a %d-byte string. " +
                    "Strings must be at least one byte.", m);
            throw new IllegalArgumentException(msg);
        }
        this.m = m;
        s = m > s.length() ? s : s.substring(0, m);
        this.s = s.replaceAll("\0*$", "");
    }

    public StringDataBox(String s) {
        this(s, s.length());
    }

    @Override
    public Type type() {
        return Type.stringType(m);
    }

    @Override
    public TypeId getTypeId() {
        return TypeId.STRING;
    }

    @Override
    public String getString() {
        return this.s;
    }

    @Override
    public byte[] toBytes() {
        // pad with null bytes
        String padded = s + new String(new char[m - s.length()]);
        return padded.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] hashBytes() {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String toString() {
        return s;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StringDataBox)) return false;
        StringDataBox other = (StringDataBox) o;
        return this.s.equals(other.s);
    }

    @Override
    public int hashCode() {
        return s.hashCode();
    }

    @Override
    public int compareTo(DataBox d) {
        if (!(d instanceof StringDataBox)) {
            String err = String.format("Invalid comparison between %s and %s.", this, d.toString());
            throw new IllegalArgumentException(err);
        }
        StringDataBox other = (StringDataBox) d;
        return this.s.compareTo(other.s);
    }
}
