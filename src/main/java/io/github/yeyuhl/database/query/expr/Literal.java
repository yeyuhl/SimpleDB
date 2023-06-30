package io.github.yeyuhl.database.query.expr;

import io.github.yeyuhl.database.databox.DataBox;
import io.github.yeyuhl.database.databox.Type;
import io.github.yeyuhl.database.table.Record;

class Literal extends Expression {
    private DataBox data;

    public Literal(DataBox data) {
        super();
        this.data = data;
    }

    @Override
    public Type getType() {
        return data.type();
    }

    @Override
    public DataBox evaluate(Record record) {
        return data;
    }

    @Override
    protected OperationPriority priority() {
        return OperationPriority.ATOMIC;
    }

    @Override
    protected String subclassString() {
        return data.toString();
    }
}