package io.github.yeyuhl.database.cli.visitor;

import io.github.yeyuhl.database.Transaction;
import io.github.yeyuhl.database.cli.parser.ASTExpression;
import io.github.yeyuhl.database.cli.parser.ASTIdentifier;
import io.github.yeyuhl.database.query.aggr.DataFunction;
import io.github.yeyuhl.database.table.Schema;

public class DeleteStatementVisitor extends StatementVisitor {
    public String tableName;
    public DataFunction cond;

    @Override
    public void visit(ASTIdentifier node, Object data) {
        this.tableName = (String) node.jjtGetValue();
    }

    @Override
    public void visit(ASTExpression node, Object data) {
        ExpressionVisitor visitor = new ExpressionVisitor();
        node.jjtAccept(visitor, data);
        this.cond = visitor.build();
    }

    @Override
    public void execute(Transaction transaction) {
        try {
            Schema schema = transaction.getSchema(tableName);
            this.cond.setSchema(schema);
            transaction.delete(tableName, cond::evaluate);
            System.out.println("DELETE");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.out.println("Failed to execute DELETE.");
        }
    }

    @Override
    public StatementType getType() {
        return StatementType.DELETE;
    }
}