package io.delta.standalone.expressions;

/**
 * An [[Expression]] that is an operator, with two properties:
 *
 * 1. The string representation is "x symbol y", rather than "funcName(x, y)".
 *
 * TODO:
 * 2. Two inputs are expected to be of the same type. If the two inputs have different types,
 *    the analyzer will find the tightest common type and do the proper type casting.
 */
public abstract class BinaryOperator implements Expression {
    protected final Expression left;
    protected final Expression right;
    protected final String symbol;

    public BinaryOperator(Expression left, Expression right, String symbol) {
        this.left = left;
        this.right = right;
        this.symbol = symbol;
    }

    @Override
    public String toString() {
        return "(" + left.toString() + " " + symbol + " " + right.toString() + ")";
    }
}
