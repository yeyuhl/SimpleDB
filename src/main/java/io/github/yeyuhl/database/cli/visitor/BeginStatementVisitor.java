package io.github.yeyuhl.database.cli.visitor;

/**
 * Purely symbolic class
 */
public class BeginStatementVisitor extends StatementVisitor {
    public StatementType getType() {
        return StatementType.BEGIN;
    }
}