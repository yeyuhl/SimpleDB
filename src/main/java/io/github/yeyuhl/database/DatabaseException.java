package io.github.yeyuhl.database;

@SuppressWarnings("serial")
public class DatabaseException extends RuntimeException {
    public DatabaseException(String message) {
        super(message);
    }

    public DatabaseException(Exception e) {
        super(e);
    }
}
