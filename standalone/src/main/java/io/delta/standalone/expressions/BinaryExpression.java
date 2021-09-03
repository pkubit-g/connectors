package io.delta.standalone.expressions;

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
    public List<Expression> children() {
        return Arrays.asList(left, right);
    }
}
