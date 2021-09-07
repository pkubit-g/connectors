package io.delta.standalone.expressions;

import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.DataType;

/**
 * A [[BinaryOperator]] that compares the left and right [[Expression]]s and returns a boolean value.
 */
public abstract class BinaryComparison extends BinaryOperator {

    public BinaryComparison(Expression left, Expression right, String symbol) {
        super(left, right, symbol);
    }

    @Override
    public DataType dataType() {
        return new BooleanType();
    }
}
