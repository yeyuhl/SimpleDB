package io.github.yeyuhl.database;

import io.github.yeyuhl.database.categories.Proj4Part2Tests;
import io.github.yeyuhl.database.categories.Proj4Tests;
import io.github.yeyuhl.database.categories.PublicTests;
import io.github.yeyuhl.database.concurrency.LockType;
import io.github.yeyuhl.database.concurrency.LoggingLockManager;
import io.github.yeyuhl.database.concurrency.ResourceName;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.*;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

import static org.junit.Assert.assertTrue;

@Category({Proj4Tests.class, Proj4Part2Tests.class})
@SuppressWarnings("resource")
public class TestDatabaseDeadlockPrecheck {
    private static final String TestDir = "testDatabaseDeadlockPrecheck";

    // 7 second max per method tested.
    public static long timeout = (long) (7000 * TimeoutScaling.factor);

    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis(timeout));

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    @Category(PublicTests.class)
    public void testDeadlock() {
        assertTrue(performCheck(tempFolder));
    }

    public static boolean performCheck(TemporaryFolder checkFolder) {
        // If we are unable to request an X lock after an X lock is requested and released, there is no point
        // running any of the later tests - every test will block the main thread.
        final ResourceName name = new ResourceName("database");

        Thread mainRunner = new Thread(() -> {
            try {
                File testDir = checkFolder.newFolder(TestDir);
                String filename = testDir.getAbsolutePath();
                LoggingLockManager lockManager = new LoggingLockManager();
                Database database = new Database(filename, 128, lockManager);
                database.setWorkMem(32);

                Transaction transactionA = database.beginTransaction();
                lockManager.acquire(transactionA.getTransactionContext(), name, LockType.X);
                transactionA.close(); // The X lock should be released here

                Transaction transactionB = database.beginTransaction();
                lockManager.acquire(transactionB.getTransactionContext(), name, LockType.X);
                transactionB.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        mainRunner.start();
        try {
            if ((new DisableOnDebug(new TestName()).isDebugging())) {
                mainRunner.join();
            } else {
                mainRunner.join(timeout);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        return mainRunner.getState() == Thread.State.TERMINATED;
    }
}
