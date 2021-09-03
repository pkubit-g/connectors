package io.delta.standalone.expressions;

import io.delta.standalone.types.DataType;

/**
 * A [[BinaryOperator]] that compares the left and right [[Expression]]s, returning a boolean value.
 */
public abstract class BinaryComparison extends BinaryOperator implements Predicate {

    public BinaryComparison(Expression left, Expression right, String symbol) {
        super(left, right, symbol);
    }

    protected int compare(Object o1, Object o2) {
        DataType compareType = compareType();
        return Util.compare(compareType, o1, o2);
    }

    private DataType compareType() {
        if (!(left instanceof Column)) return left.dataType();
        if (!(right instanceof Column)) return right.dataType();
        throw new RuntimeException("BinaryComparison::compareType > can't compare two Columns");
    }
}
