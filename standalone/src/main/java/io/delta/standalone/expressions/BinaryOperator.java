package io.delta.standalone.expressions;

public abstract class BinaryOperator implements Expression {
    protected final Expression left;
    protected final Expression right;

    // TODO protected final DataType inputType;

    protected final String symbol;

    public BinaryOperator(Expression left, Expression right, String symbol) {
        this.left = left;
        this.right = right;
        this.symbol = symbol;
    }

    @Override
    public String treeString() {
        return "(" + left.treeString() + " " + symbol + " " + right.treeString() + ")";
    }
}
