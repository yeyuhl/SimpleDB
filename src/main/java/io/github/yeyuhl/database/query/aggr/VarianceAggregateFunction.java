package io.github.yeyuhl.database.query.aggr;

import io.github.yeyuhl.database.databox.DataBox;
import io.github.yeyuhl.database.databox.impl.FloatDataBox;
import io.github.yeyuhl.database.databox.Type;
import io.github.yeyuhl.database.databox.TypeId;

/**
 * A VAR aggregate keeps track of the variance across all the elements it has
 * seen so far, and returns that variance as a result. Undefined for the STRING
 * data type. Always returns a result of data type FLOAT. If only one value has
 * been seen, the result will be zero.
 *
 * Implementation based off of Welford's Online Algorithm for computing
 * variance.
 */
public class VarianceAggregateFunction extends AggregateFunction {
    double M = 0.0;
    double S = 0.0;
    int k = 0;

    public VarianceAggregateFunction(Type colType) {
        if (colType.getTypeId() == TypeId.STRING) {
            throw new IllegalArgumentException("Invalid data type for VAR aggregate: STRING");
        }
    }

    @Override
    public void update(DataBox d) {
        k++;
        float x = 0;
        switch (d.getTypeId()) {
            case BOOL:
                x = d.getBool() ? 1 : 0;
                break;
            case INT:
                x = d.getInt();
                break;
            case LONG:
                x = d.getLong();
                break;
            case FLOAT:
                x = d.getFloat();
                break;
            case STRING:
                throw new IllegalArgumentException("Can't compute variance of a String");
        }
        double delta = x - M;
        M += delta / k;
        S += delta * (x - M);
    }

    @Override
    public DataBox getResult() {
        if (k <= 1) return new FloatDataBox(0);
        Double result = M / (k - 1);
        return new FloatDataBox(result.floatValue());
    }

    @Override
    public Type getResultType() {
        return Type.floatType();
    }

    @Override
    public void reset() {
        this.M = 0.0;
        this.S = 0.0;
        this.k = 0;
    }
}
