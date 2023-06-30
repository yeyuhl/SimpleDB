package io.github.yeyuhl.database.cli.visitor;

import io.github.yeyuhl.database.Transaction;
import io.github.yeyuhl.database.cli.parser.ASTIdentifier;


import java.io.PrintStream;

class SavepointStatementVisitor extends StatementVisitor {
    public String savepointName;

    @Override
    public void visit(ASTIdentifier node, Object data) {
        this.savepointName = (String) node.jjtGetValue();
    }

    @Override
    public void execute(Transaction transaction, PrintStream out) {
        transaction.savepoint(savepointName);
    }

    @Override
    public StatementType getType() {
        return StatementType.SAVEPOINT;
    }
}