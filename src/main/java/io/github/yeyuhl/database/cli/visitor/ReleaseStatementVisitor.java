package io.github.yeyuhl.database.cli.visitor;

import io.github.yeyuhl.database.Transaction;
import io.github.yeyuhl.database.cli.parser.ASTReleaseStatement;

public class ReleaseStatementVisitor extends StatementVisitor {
    public String savepointName;

    @Override
    public void visit(ASTReleaseStatement node, Object data) {
        this.savepointName = (String) node.jjtGetValue();
    }

    @Override
    public void execute(Transaction transaction) {
        try {
            transaction.releaseSavepoint(savepointName);
            System.out.println("RELEASE SAVEPOINT " + savepointName);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.out.println("Failed to execute RELEASE SAVEPOINT.");
        }
    }

    @Override
    public StatementType getType() {
        return StatementType.RELEASE_SAVEPOINT;
    }

}