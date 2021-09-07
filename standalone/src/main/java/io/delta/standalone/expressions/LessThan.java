package io.delta.standalone.expressions;

/**
 * Usage: new LessThan(expr1, expr2) - Returns true if `expr1` is less than `expr2`, else false.
 */
public final class LessThan extends BinaryComparison {
    public LessThan(Expression left, Expression right) {
        super(left, right, "<");
    }

    @Override
    public Object nullSafeBoundEval(Object leftResult, Object rightResult) {
        return Util.compare(left.dataType(), leftResult, rightResult) < 0;
    }
}
