package io.delta.standalone.expressions;

/**
 * A [[BinaryExpression]] that is an operator, with two properties:
 *
 * 1. The string representation is "x symbol y", rather than "funcName(x, y)".
 */
public abstract class BinaryOperator extends BinaryExpression {
    protected final String symbol;

    public BinaryOperator(Expression left, Expression right, String symbol) {
        super(left, right);
        this.symbol = symbol;
    }

    @Override
    public String toString() {
        return "(" + left.toString() + " " + symbol + " " + right.toString() + ")";
    }
}
