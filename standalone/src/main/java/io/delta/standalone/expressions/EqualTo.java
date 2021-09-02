package io.delta.standalone.expressions;

public class EqualTo extends BinaryOperator implements Predicate {
    public EqualTo(Expression left, Expression right) {
        super(left, right, "=");
    }

    @Override
    public BooleanLiteral eval() {
        Expression leftResult = left.eval();
        Expression rightResult = right.eval();
        return null; // TODO
    }
}
