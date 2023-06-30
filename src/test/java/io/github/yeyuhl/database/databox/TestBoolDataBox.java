package io.github.yeyuhl.database.databox;

import io.github.yeyuhl.database.categories.Proj99Tests;
import io.github.yeyuhl.database.categories.SystemTests;
import io.github.yeyuhl.database.common.ByteBuffer;
import io.github.yeyuhl.database.databox.impl.BoolDataBox;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category({Proj99Tests.class, SystemTests.class})
public class TestBoolDataBox {
    @Test
    public void testType() {
        assertEquals(Type.boolType(), new BoolDataBox(true).type());
        assertEquals(Type.boolType(), new BoolDataBox(false).type());
    }

    @Test
    public void testGetBool() {
        assertEquals(true, new BoolDataBox(true).getBool());
        assertEquals(false, new BoolDataBox(false).getBool());
    }

    @Test(expected = RuntimeException.class)
    public void testGetInt() {
        new BoolDataBox(true).getInt();
    }

    @Test(expected = RuntimeException.class)
    public void testGetLong() {
        new BoolDataBox(true).getLong();
    }

    @Test(expected = RuntimeException.class)
    public void testGetFloat() {
        new BoolDataBox(true).getFloat();
    }

    @Test(expected = RuntimeException.class)
    public void testGetString() {
        new BoolDataBox(true).getString();
    }

    @Test
    public void testToAndFromBytes() {
        for (boolean b : new boolean[] {true, false}) {
            BoolDataBox d = new BoolDataBox(b);
            byte[] bytes = d.toBytes();
            assertEquals(d, DataBox.fromBytes(ByteBuffer.wrap(bytes), Type.boolType()));
        }
    }

    @Test
    public void testEquals() {
        BoolDataBox tru = new BoolDataBox(true);
        BoolDataBox fls = new BoolDataBox(false);
        assertEquals(tru, tru);
        assertEquals(fls, fls);
        assertNotEquals(tru, fls);
        assertNotEquals(fls, tru);
    }

    @Test
    public void testCompareTo() {
        BoolDataBox tru = new BoolDataBox(true);
        BoolDataBox fls = new BoolDataBox(false);
        assertTrue(fls.compareTo(fls) == 0);
        assertTrue(fls.compareTo(tru) < 0);
        assertTrue(tru.compareTo(tru) == 0);
        assertTrue(tru.compareTo(fls) > 0);
    }
}
