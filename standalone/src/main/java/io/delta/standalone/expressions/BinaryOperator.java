package io.delta.standalone.expressions;

import io.delta.standalone.types.AbstractDataType;

public abstract class BinaryOperator implements Expression {
    protected final Expression left;
    protected final Expression right;
    protected final AbstractDataType inputType;
    protected final String symbol;

    public BinaryOperator(Expression left, Expression right, AbstractDataType inputType, String symbol) {
        this.left = left;
        this.right = right;
        this.inputType = inputType;
        this.symbol = symbol;
    }

    @Override
    public String treeString() {
        return "(" + left.treeString() + " " + symbol + " " + right.treeString() + ")";
    }
}
