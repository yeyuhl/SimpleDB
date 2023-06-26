package io.github.yeyuhl.database.databox;

import io.github.yeyuhl.database.common.Buffer;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * 数据库数据类型，包括int，bool，float，long，string，byte_array
 * 当n!=m，n bytes的String和m bytes的String是不同的类型
 *
 * @author yeyuhl
 * @since 2023/6/20
 */
public class Type {
    /**
     * 该type的类型
     */
    private TypeId typeId;

    /**
     * 该type的大小，按字节计算
     */
    private int sizeInBytes;

    public Type(TypeId typeId, int sizeInBytes) {
        this.typeId = typeId;
        this.sizeInBytes = sizeInBytes;
    }

    public static Type boolType() {
        // 在Java中，boolean类型的大小是1bit，但是我们的数据库中，bool类型的大小强制设定为1byte
        return new Type(TypeId.BOOL, 1);
    }

    public static Type intType() {
        return new Type(TypeId.INT, Integer.BYTES);
    }

    public static Type floatType() {
        return new Type(TypeId.FLOAT, Float.BYTES);
    }

    public static Type stringType(int n) {
        if (n < 0) {
            String msg = String.format("The provided string length %d is negative.", n);
            throw new IllegalArgumentException(msg);
        }
        if (n == 0) {
            String msg = "Empty strings are not supported.";
            throw new IllegalArgumentException(msg);
        }
        return new Type(TypeId.STRING, n);
    }

    public static Type longType() {
        return new Type(TypeId.LONG, Long.BYTES);
    }

    public static Type byteArrayType(int n) {
        return new Type(TypeId.BYTE_ARRAY, n);
    }

    public TypeId getTypeId() {
        return typeId;
    }

    public int getSizeInBytes() {
        return sizeInBytes;
    }

    public byte[] toBytes() {
        // 我们将typeId和sizeInBytes分两部分存储在byte数组中
        // 比如42 byte的String，表示为[3,42]，其类型ID是3，大小是42
        ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES * 2);
        buf.putInt(typeId.ordinal());
        buf.putInt(sizeInBytes);
        return buf.array();
    }

    public static Type fromBytes(Buffer buf) {
        int ordinal = buf.getInt();
        int sizeInBytes = buf.getInt();
        switch (TypeId.fromInt(ordinal)) {
            case BOOL:
                assert (sizeInBytes == 1);
                return Type.boolType();
            case INT:
                assert (sizeInBytes == Integer.BYTES);
                return Type.intType();
            case FLOAT:
                assert (sizeInBytes == Float.BYTES);
                return Type.floatType();
            case STRING:
                return Type.stringType(sizeInBytes);
            case LONG:
                assert (sizeInBytes == Long.BYTES);
                return Type.longType();
            case BYTE_ARRAY:
                return Type.byteArrayType(sizeInBytes);
            default:
                throw new RuntimeException("unreachable");
        }
    }

    public static Type fromString(String s) {
        String type = s;
        int openIndex = s.indexOf("(");
        if (openIndex > 0) type = s.substring(0, openIndex);
        type = type.trim().toLowerCase();
        switch (type) {
            case "int":
                ;
            case "integer":
                return intType();
            case "char":
            case "varchar":
            case "string":
                int closeIndex = s.indexOf(")");
                if (closeIndex < 0 || openIndex < 0) {
                    throw new IllegalArgumentException("Malformed type string: " + s);
                }
                String size = s.substring(openIndex + 1, closeIndex).trim();
                return Type.stringType(Integer.parseInt(size));
            case "float":
                return Type.floatType();
            case "long":
                return Type.longType();
            case "bool":
            case "boolean":
                return Type.boolType();
            default:
                throw new RuntimeException("Unknown type: " + type);
        }
    }

    @Override
    public String toString() {
        return String.format("(%s, %d)", typeId.toString(), sizeInBytes);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof Type)) {
            return false;
        }
        Type t = (Type) o;
        return typeId.equals(t.typeId) && sizeInBytes == t.sizeInBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeId, sizeInBytes);
    }
}
