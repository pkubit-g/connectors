package io.delta.standalone.expressions;

import io.delta.standalone.data.RowRecord;
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

    @Override
    public abstract Boolean eval(RowRecord record);

    protected int compare(Object leftResult, Object rightResult) {
        if (left.bound() && right.bound()) {
            return Util.compare(left.dataType(), leftResult, rightResult);
        }

        throw new RuntimeException("BinaryComparison::compareType > can't compare two unbound Expressions");
    }
}
