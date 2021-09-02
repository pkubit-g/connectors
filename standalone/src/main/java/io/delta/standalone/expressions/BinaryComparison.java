package io.delta.standalone.expressions;

import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.IntegerType;

import java.util.Comparator;

public abstract class BinaryComparison extends BinaryOperator implements Predicate {

    public BinaryComparison(Expression left, Expression right, String symbol) {
        super(left, right, new AnyDataType(), symbol);
    }

    protected int compare(Object o1, Object o2) {
        if (inputType instanceof BooleanType) {
            return Comparator.<Boolean>naturalOrder().compare((Boolean) o1, (Boolean) o2);
        }
        if (inputType instanceof IntegerType) {
            return Comparator.<Integer>naturalOrder().compare((Integer) o1, (Integer) o2);
        }

        throw new UnsupportedOperationException("BinaryComparison>::compare > unrecognized inputType: " + inputType.toString());
    }
}
