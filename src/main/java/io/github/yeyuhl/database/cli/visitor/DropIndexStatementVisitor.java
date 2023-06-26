package io.github.yeyuhl.database.cli.visitor;

import io.github.yeyuhl.database.Transaction;
import io.github.yeyuhl.database.cli.parser.ASTColumnName;
import io.github.yeyuhl.database.cli.parser.ASTIdentifier;

public class DropIndexStatementVisitor extends StatementVisitor {
    public String tableName;
    public String columnName;

    @Override
    public void visit(ASTColumnName node, Object data) {
        this.columnName = (String) node.jjtGetValue();
    }

    @Override
    public void visit(ASTIdentifier node, Object data) {
        this.tableName = (String) node.jjtGetValue();
    }

    @Override
    public void execute(Transaction transaction) {
        try {
            transaction.dropIndex(tableName, columnName);
            System.out.printf("DROP INDEX %s(%s)\n", tableName, columnName);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.out.println("Failed to execute DROP INDEX.");
        }
    }

    @Override
    public StatementType getType() {
        return StatementType.DROP_INDEX;
    }
}