package io.delta.standalone.expressions;

import io.delta.standalone.data.RowRecord;

/**
 * Usage: new LessThan(expr1, expr2) - Returns true if `expr1` is less than `expr2`, else false.
 */
public class LessThan extends BinaryComparison {
    public LessThan(Expression left, Expression right) {
        super(left, right, "<");
    }

    @Override
    public Boolean eval(RowRecord record) {
        Object leftResult = left.eval(record);
        Object rightResult = right.eval(record);
        return compare(leftResult, rightResult) < 0;
    }
}
