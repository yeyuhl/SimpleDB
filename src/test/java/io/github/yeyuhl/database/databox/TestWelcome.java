package io.github.yeyuhl.database.databox;

import io.github.yeyuhl.database.categories.Proj0Tests;
import io.github.yeyuhl.database.categories.PublicTests;
import io.github.yeyuhl.database.databox.impl.StringDataBox;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category({Proj0Tests.class, PublicTests.class})
public class TestWelcome {
    @Test
    public void testComplete() {
        assertEquals("welcome", new StringDataBox("welcome", 7).toString());
    }
}
