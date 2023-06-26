package io.github.yeyuhl.database.query.aggr;

import io.github.yeyuhl.database.databox.DataBox;
import io.github.yeyuhl.database.databox.Type;

/**
 * A MAX aggregate keeps track of the largest value it has seen and return that
 * value as a result. Works for all data types and always returns the same data
 * type as the column being aggregated.
 */
public class MaxAggregateFunction extends AggregateFunction {
    private Type colType;
    private DataBox max;

    public MaxAggregateFunction(Type colType) {
        this.colType = colType;
    }

    @Override
    public void update(DataBox d) {
        if (max == null || d.compareTo(max) > 0) max = d;
    }

    @Override
    public DataBox getResult() {
        return max;
    }

    @Override
    public Type getResultType() {
        return colType;
    }

    @Override
    public void reset() {
        this.max = null;
    }

}
