package io.github.yeyuhl.database.cli.visitor;

public enum StatementType{
    CREATE_TABLE,
    CREATE_INDEX,
    DROP_TABLE,
    DROP_INDEX,
    SELECT,
    INSERT,
    DELETE,
    UPDATE,
    BEGIN,
    COMMIT,
    ROLLBACK,
    SAVEPOINT,
    RELEASE_SAVEPOINT,
    EXPLAIN
}