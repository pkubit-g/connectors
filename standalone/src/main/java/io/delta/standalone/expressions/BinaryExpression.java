package io.delta.standalone.expressions;

import io.delta.standalone.data.RowRecord;

import java.util.Arrays;
import java.util.List;

/**
 * An expression with two inputs and one output.
 */
public abstract class BinaryExpression extends Expression {
    protected final Expression left;
    protected final Expression right;

    public BinaryExpression(Expression left, Expression right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public final Object eval(RowRecord record) {
        Object leftResult = left.eval(record);
        if (null == leftResult) {
            throw new RuntimeException("BinaryExpression can't operate on a null child");
        }
        if (!left.bound()) {
            throw new RuntimeException("BinaryExpression can't operate on unbound child");
        }

        Object rightResult = right.eval(record);
        if (null == rightResult) {
            throw new RuntimeException("BinaryExpression can't operate on a null child");
        }
        if (!right.bound()) {
            throw new RuntimeException("BinaryExpression can't operate on unbound child");
        }

        return nullSafeBoundEval(leftResult, rightResult);
    }

    protected abstract Object nullSafeBoundEval(Object leftResult, Object rightResult);

    @Override
    public List<Expression> children() {
        return Arrays.asList(left, right);
    }
}
