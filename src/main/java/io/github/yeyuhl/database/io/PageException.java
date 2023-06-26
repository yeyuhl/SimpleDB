package io.github.yeyuhl.database.io;

/**
 * Exception thrown for errors while paging.
 */
@SuppressWarnings("serial")
public class PageException extends RuntimeException {
    public PageException() {
        super();
    }

    public PageException(Exception e) {
        super(e);
    }

    public PageException(String message) {
        super(message);
    }
}

