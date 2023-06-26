package io.github.yeyuhl.database.cli.visitor;

import io.github.yeyuhl.database.Transaction;
import io.github.yeyuhl.database.cli.parser.RookieParserDefaultVisitor;
import io.github.yeyuhl.database.query.QueryPlan;

import java.util.Optional;

public abstract class StatementVisitor extends RookieParserDefaultVisitor {
    public void execute(Transaction transaction) {
        throw new UnsupportedOperationException("Statement is not executable.");
    }

    public Optional<String> getSavepointName() {
        return Optional.empty();
    }

    public Optional<QueryPlan> getQueryPlan(Transaction transaction) {
        return Optional.empty();
    }

    public abstract StatementType getType();
}