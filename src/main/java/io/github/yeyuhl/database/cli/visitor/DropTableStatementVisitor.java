package io.github.yeyuhl.database.cli.visitor;

import io.github.yeyuhl.database.Transaction;
import io.github.yeyuhl.database.cli.parser.ASTIdentifier;


import java.io.PrintStream;

class DropTableStatementVisitor extends StatementVisitor {
    public String tableName;

    @Override
    public void visit(ASTIdentifier node, Object data) {
        this.tableName = (String) node.jjtGetValue();
    }

    @Override
    public void execute(Transaction transaction, PrintStream out) {
        try {
            transaction.dropTable(this.tableName);
            out.println("DROP TABLE " + this.tableName + ";");
        } catch (Exception e) {
            out.println(e.getMessage());
            out.println("Failed to execute DROP TABLE.");
        }
    }

    @Override
    public StatementType getType() {
        return StatementType.DROP_TABLE;
    }

}