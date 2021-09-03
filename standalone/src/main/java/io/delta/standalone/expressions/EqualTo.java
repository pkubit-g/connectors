package io.delta.standalone.expressions;

import io.delta.standalone.data.RowRecord;

/**
 * Usage: new EqualTo(expr1, expr2) - Returns true if `expr1` equals `expr2`, else false.
 */
public class EqualTo extends BinaryComparison {
    public EqualTo(Expression left, Expression right) {
        super(left, right, "=");
    }

    @Override
    public Boolean eval(RowRecord record) {
        Object leftResult = left.eval(record);
        Object rightResult = right.eval(record);
        return compare(leftResult, rightResult) == 0;
    }
}
