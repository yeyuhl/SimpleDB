package io.github.yeyuhl.database.cli.visitor;

/**
 * Purely symbolic class
 */
public class CommitStatementVisitor extends StatementVisitor {
    public StatementType getType() {
        return StatementType.COMMIT;
    }
}