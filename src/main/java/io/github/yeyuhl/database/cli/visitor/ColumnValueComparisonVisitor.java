package io.github.yeyuhl.database.cli.visitor;

import io.github.yeyuhl.database.cli.PrettyPrinter;
import io.github.yeyuhl.database.cli.parser.ASTColumnName;
import io.github.yeyuhl.database.cli.parser.ASTComparisonOperator;
import io.github.yeyuhl.database.cli.parser.ASTLiteral;
import io.github.yeyuhl.database.cli.parser.RookieParserDefaultVisitor;
import io.github.yeyuhl.database.common.PredicateOperator;
import io.github.yeyuhl.database.databox.DataBox;


class ColumnValueComparisonVisitor extends RookieParserDefaultVisitor {
    PredicateOperator op;
    String columnName;
    DataBox value;

    @Override
    public void visit(ASTLiteral node, Object data) {
        this.value = PrettyPrinter.parseLiteral((String) node.jjtGetValue());
    }

    @Override
    public void visit(ASTColumnName node, Object data) {
        this.columnName = (String) node.jjtGetValue();
        // keep things in format columnName <= value
        if (this.op != null) this.op = op.reverse();
    }

    @Override
    public void visit(ASTComparisonOperator node, Object data) {
        this.op = PredicateOperator.fromSymbol((String) node.jjtGetValue());
    }
}
