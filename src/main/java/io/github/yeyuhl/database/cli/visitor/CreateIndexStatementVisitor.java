package io.github.yeyuhl.database.cli.visitor;

import io.github.yeyuhl.database.Transaction;
import io.github.yeyuhl.database.cli.parser.ASTColumnName;
import io.github.yeyuhl.database.cli.parser.ASTIdentifier;

public class CreateIndexStatementVisitor extends StatementVisitor {
    public String tableName;
    public String columnName;

    @Override
    public void execute(Transaction transaction) {
        transaction.createIndex(tableName, columnName, false);
        System.out.printf("CREATE INDEX ON %s (%s)\n", tableName, columnName);
    }

    @Override
    public void visit(ASTIdentifier node, Object data) {
        this.tableName = (String) node.jjtGetValue();
    }

    @Override
    public void visit(ASTColumnName node, Object data) {
        this.columnName = (String) node.jjtGetValue();
    }

    @Override
    public StatementType getType() {
        return StatementType.CREATE_INDEX;
    }
}