package io.github.yeyuhl.database.databox;

import io.github.yeyuhl.database.common.Buffer;
import io.github.yeyuhl.database.databox.impl.*;

import java.nio.charset.StandardCharsets;

/**
 * DataBox的抽象类，DataBox是对Java数据类型的封装，使其成为数据库的数据容器
 * 一个DataBox可以是int，boolean，float，Long，String
 *
 * @author yeyuhl
 * @since 2023/6/20
 */
public abstract class DataBox implements Comparable<DataBox> {
    public abstract Type type();

    public abstract TypeId getTypeId();

    public boolean getBool() {
        throw new RuntimeException("not boolean type");
    }

    public int getInt() {
        throw new RuntimeException("not int type");
    }

    public float getFloat() {
        throw new RuntimeException("not float type");
    }

    public String getString() {
        throw new RuntimeException("not String type");
    }

    public long getLong() {
        throw new RuntimeException("not Long type");
    }

    public byte[] getByteArray() {
        throw new RuntimeException("not Byte Array type");
    }

    /**
     * 将DataBox序列化，即转为byte数组存储起来，反序列化时需要提供DataBox的类型
     */
    public abstract byte[] toBytes();

    /**
     * 对于String类型的DataBox，需要特殊处理，因为String类型的DataBox的大小是可变的
     */
    public byte[] hashBytes() {
        return toBytes();
    }

    public static DataBox fromBytes(Buffer buf, Type type) {
        switch (type.getTypeId()) {
            case BOOL: {
                byte b = buf.get();
                assert (b == 0 || b == 1);
                return new BoolDataBox(b == 1);
            }
            case INT: {
                return new IntDataBox(buf.getInt());
            }
            case FLOAT: {
                return new FloatDataBox(buf.getFloat());
            }
            case STRING: {
                byte[] bytes = new byte[type.getSizeInBytes()];
                buf.get(bytes);
                String s = new String(bytes, StandardCharsets.UTF_8);
                return new StringDataBox(s, type.getSizeInBytes());
            }
            case LONG: {
                return new LongDataBox(buf.getLong());
            }
            case BYTE_ARRAY: {
                byte[] bytes = new byte[type.getSizeInBytes()];
                buf.get(bytes);
                return new ByteArrayDataBox(bytes, type.getSizeInBytes());
            }
            default: {
                String err = String.format("Unhandled TypeId %s.",
                        type.getTypeId().toString());
                throw new IllegalArgumentException(err);
            }
        }
    }

    public static DataBox fromString(Type type, String s) {
        String raw = s;
        s = s.toLowerCase().trim();
        switch (type.getTypeId()) {
            case BOOL:
                return new BoolDataBox(s.equals("true"));
            case INT:
                return new IntDataBox(Integer.parseInt(s));
            case LONG:
                return new LongDataBox(Long.parseLong(s));
            case FLOAT:
                return new FloatDataBox(Float.parseFloat(s));
            case STRING:
                return new StringDataBox(raw);
            default:
                throw new RuntimeException("Unreachable code");
        }
    }

    /**
     * 将Object类型的数据转换为DataBox，即对支持的数据类型进行封装
     * 举个例子：
     * DataBox.fromObject(186) ==  new IntDataBox(186)
     * DataBox.fromObject("186") == new StringDataBox("186")
     * DataBox.fromObject(new ArrayList<>()) // Error! ArrayList 不支持
     *
     * @param o Object类型的数据
     * @return DataBox
     */
    public static DataBox fromObject(Object o) {
        if (o instanceof DataBox) {
            return (DataBox) o;
        }
        if (o instanceof Integer) {
            return new IntDataBox((Integer) o);
        }
        if (o instanceof String) {
            return new StringDataBox((String) o);
        }
        if (o instanceof Boolean) {
            return new BoolDataBox((Boolean) o);
        }
        if (o instanceof Long) {
            return new LongDataBox((Long) o);
        }
        if (o instanceof Float) {
            return new FloatDataBox((Float) o);
        }
        if (o instanceof Double) {
            // 隐式转换
            double d = (Double) o;
            return new FloatDataBox((float) d);
        }
        if (o instanceof byte[]) {
            return new ByteArrayDataBox((byte[]) o, ((byte[]) o).length);
        }
        throw new IllegalArgumentException("Object was not a supported data type");
    }
}
