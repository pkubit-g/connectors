package io.delta.standalone.expressions;

import java.util.Collections;
import java.util.List;

/**
 * An expression with one input and one output. The output is by default evaluated to null
 * if the input is evaluated to null.
 */
public abstract class UnaryExpression implements Expression {
    protected final Expression child;

    public UnaryExpression(Expression child) {
        this.child = child;
    }

    @Override
    public List<Expression> children() {
        return Collections.singletonList(child);
    }
}
