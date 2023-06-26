package io.github.yeyuhl.database.query.aggr;

import io.github.yeyuhl.database.databox.DataBox;
import io.github.yeyuhl.database.databox.Type;

/**
 * A LAST aggregate keeps track of the last value it has seen and returns that
 * as a result. Works for all data types, and always returns the same data type
 * as the column being aggregated on.
 */
public class LastAggregateFunction extends AggregateFunction {
    Type colType;
    DataBox last;

    public LastAggregateFunction(Type columnType) {
        this.colType = columnType;
    }

    @Override
    public void update(DataBox value) {
        this.last = value;
    }

    @Override
    public DataBox getResult() {
        return this.last;
    }

    @Override
    public Type getResultType() {
        return colType;
    }

    @Override
    public void reset() {
        this.last = null;
    }
}
