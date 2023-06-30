package io.github.yeyuhl.database.databox;

import io.github.yeyuhl.database.categories.Proj99Tests;
import io.github.yeyuhl.database.categories.SystemTests;
import io.github.yeyuhl.database.common.ByteBuffer;
import io.github.yeyuhl.database.databox.impl.LongDataBox;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category({Proj99Tests.class, SystemTests.class})
public class TestLongDataBox {
    @Test
    public void testType() {
        assertEquals(Type.longType(), new LongDataBox(0L).type());
    }

    @Test(expected = RuntimeException.class)
    public void testGetBool() {
        new LongDataBox(0L).getBool();
    }

    @Test(expected = RuntimeException.class)
    public void testGetInt() {
        new LongDataBox(0L).getInt();
    }

    @Test
    public void testGetLong() {
        assertEquals(0L, new LongDataBox(0L).getLong());
    }

    @Test(expected = RuntimeException.class)
    public void testGetFloat() {
        new LongDataBox(0L).getFloat();
    }

    @Test(expected = RuntimeException.class)
    public void testGetString() {
        new LongDataBox(0L).getString();
    }

    @Test
    public void testToAndFromBytes() {
        for (long i = -10L; i < 10L; ++i) {
            LongDataBox d = new LongDataBox(i);
            byte[] bytes = d.toBytes();
            assertEquals(d, DataBox.fromBytes(ByteBuffer.wrap(bytes), Type.longType()));
        }
    }

    @Test
    public void testEquals() {
        LongDataBox zero = new LongDataBox(0L);
        LongDataBox one = new LongDataBox(1L);
        assertEquals(zero, zero);
        assertEquals(one, one);
        assertNotEquals(zero, one);
        assertNotEquals(one, zero);
    }

    @Test
    public void testCompareTo() {
        LongDataBox zero = new LongDataBox(0L);
        LongDataBox one = new LongDataBox(1L);
        assertTrue(zero.compareTo(zero) == 0L);
        assertTrue(zero.compareTo(one) < 0L);
        assertTrue(one.compareTo(one) == 0L);
        assertTrue(one.compareTo(zero) > 0L);
    }
}
