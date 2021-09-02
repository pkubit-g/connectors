package io.delta.standalone.expressions;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.BooleanType;

public class And extends BinaryOperator implements Predicate {

    public And(Expression left, Expression right) {
        super(left, right, new BooleanType(), "&&");
    }

    @Override
    public Boolean eval(RowRecord record) {
        // TODO: lazy eval
        Object leftResult = left.eval(record);
        Object rightResult = right.eval(record);

        if (null == leftResult || null == rightResult) {
            throw new RuntimeException("'And' expression children.eval results can't be null");
        }
        if (!(leftResult instanceof Boolean) || !(rightResult instanceof Boolean)) {
            throw new RuntimeException("'And' expression children.eval results must be Booleans");
        }

        return (boolean) leftResult && (boolean) rightResult;
    }
}
