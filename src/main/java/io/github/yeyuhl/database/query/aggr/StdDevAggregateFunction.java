package io.github.yeyuhl.database.query.aggr;

import io.github.yeyuhl.database.databox.DataBox;
import io.github.yeyuhl.database.databox.impl.FloatDataBox;
import io.github.yeyuhl.database.databox.Type;
import io.github.yeyuhl.database.databox.TypeId;
/**
 * A STDDEV aggregate maintains a VAR aggregate and returns the square root of
 * the variance as a result. Undefined for the STRING data type. Always
 * returns a FLOAT type result. If only one value has been seen, the result will
 * be zero.
 */
public class StdDevAggregateFunction extends AggregateFunction {
    private VarianceAggregateFunction varAgg;
    public StdDevAggregateFunction(Type colType) {
        if (colType.getTypeId() == TypeId.STRING) {
            throw new IllegalArgumentException("Invalid data type for STDDEV aggregate: STRING");
        }
        this.varAgg = new VarianceAggregateFunction(colType);
    }

    @Override
    public void update(DataBox value) {
        this.varAgg.update(value);
    }

    @Override
    public DataBox getResult() {
        Double result = Math.sqrt(varAgg.getResult().getFloat());
        return new FloatDataBox(result.floatValue());
    }

    @Override
    public Type getResultType() {
        return Type.floatType();
    }

    @Override
    public void reset() {
        varAgg.reset();
    }
}
