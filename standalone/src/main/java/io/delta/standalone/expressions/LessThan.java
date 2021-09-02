package io.delta.standalone.expressions;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.IntegerType;

import java.util.Comparator;

public class LessThan extends BinaryComparison {
    public LessThan(Expression left, Expression right) {
        super(left, right, "=");
    }

    @Override
    public Boolean eval(RowRecord record) {
        Object leftResult = left.eval(record);
        Object rightResult = right.eval(record);
        return compare(leftResult, rightResult) < 0;
    }
}
