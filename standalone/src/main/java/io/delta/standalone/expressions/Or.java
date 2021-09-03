package io.delta.standalone.expressions;

import io.delta.standalone.data.RowRecord;

/**
 * Usage: new Or(expr1, expr2) - Logical OR
 */
public class Or extends BinaryOperator implements Predicate {

    public Or(Expression left, Expression right) {
        super(left, right, "||");
    }

    @Override
    public Boolean eval(RowRecord record) {
        // TODO: lazy eval
        Object leftResult = left.eval(record);
        Object rightResult = right.eval(record);

        if (null == leftResult || null == rightResult) {
            throw new RuntimeException("'Or' expression children.eval results can't be null");
        }
        if (!(leftResult instanceof Boolean) || !(rightResult instanceof Boolean)) {
            throw new RuntimeException("'Or' expression children.eval results must be Booleans");
        }

        return (boolean) leftResult || (boolean) rightResult;
    }
}
