package io.delta.standalone.expressions;

public class And extends BinaryOperator implements Predicate {

    public And(Expression left, Expression right) {
        super(left, right, "&&");
    }

    @Override
    public BooleanLiteral eval() {
        // TODO: lazy eval
        Expression leftResult = left.eval();
        Expression rightResult = right.eval();

        if (!(leftResult instanceof BooleanLiteral) || !(rightResult instanceof BooleanLiteral)) {
            throw new RuntimeException("'And' expression children.eval results must be BooleanLiteral");
        }

        BooleanLiteral leftResultLiteral = (BooleanLiteral) leftResult;
        BooleanLiteral rightResultLiteral = (BooleanLiteral) rightResult;

        return new BooleanLiteral(leftResultLiteral.value() && rightResultLiteral.value());
    }
}
