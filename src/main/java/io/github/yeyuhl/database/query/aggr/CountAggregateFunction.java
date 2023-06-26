package io.github.yeyuhl.database.query.aggr;

import io.github.yeyuhl.database.databox.DataBox;
import io.github.yeyuhl.database.databox.impl.IntDataBox;
import io.github.yeyuhl.database.databox.Type;

/**
 * A COUNT aggregate counts the number of values passed in and returns the total
 * as a result. Works for all data types and always returns an INT type result.
 */
public class CountAggregateFunction extends AggregateFunction {
    private int count = 0;
    @Override
    public void update(DataBox d) {
        count++;
    }

    @Override
    public DataBox getResult() {
        return new IntDataBox(count);
    }

    @Override
    public Type getResultType() {
        return Type.intType();
    }

    @Override
    public void reset() {
        this.count = 0;
    }
}