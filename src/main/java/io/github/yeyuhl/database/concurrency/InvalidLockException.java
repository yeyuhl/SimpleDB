package io.github.yeyuhl.database.concurrency;

@SuppressWarnings("serial")
public class InvalidLockException extends RuntimeException {
    InvalidLockException(String message) {
        super(message);
    }
}

