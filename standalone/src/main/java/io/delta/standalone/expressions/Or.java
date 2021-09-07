package io.delta.standalone.expressions;

/**
 * Usage: new Or(expr1, expr2) - Logical OR
 */
public final class Or extends BinaryComparison {

    public Or(Expression left, Expression right) {
        super(left, right, "||");
    }

    @Override
    public Object nullSafeBoundEval(Object leftResult, Object rightResult) {
        if (!(leftResult instanceof Boolean) || !(rightResult instanceof Boolean)) {
            throw new RuntimeException("'Or' expression children.eval results must be Booleans");
        }

        return (boolean) leftResult || (boolean) rightResult;
    }
}
