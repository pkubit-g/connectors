package io.delta.standalone.expressions;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.DataType;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Usage: new In(expr, literalList) - Returns true if `expr` is equal to any in `expr`, else false.
 */
public class In implements Predicate {
    private final Expression value;
    private final List<Literal> elems;

    public In(Expression value, List<Literal> elems) {
        if (null == value) {
            throw new IllegalArgumentException("In 'value' cannot be null");
        }
        if (null == elems) {
            throw new IllegalArgumentException("In 'elems' cannot be null");
        }
        if (elems.isEmpty()) {
            throw new IllegalArgumentException("In 'elems' cannot be empty");
        }

        DataType firstType = elems.get(0).dataType();
        boolean allSameDataType = elems.stream().allMatch(x -> x.dataType().equals(firstType));

        if (!allSameDataType) {
            throw new IllegalArgumentException("In 'elems' must all be of the same DataType");
        }

        this.value = value;
        this.elems = elems;
    }

    @Override
    public Boolean eval(RowRecord record) {
        Object result = value.eval(record);

        if (!elems.get(0).dataType().equals(value.dataType())) {
            throw new IllegalArgumentException("In 'value' DataType must be the same as 'elems'");
        }

        return elems.stream().anyMatch(setElem -> {
            Object setElemValue= setElem.value();
            return Util.compare(value.dataType(), result, setElemValue) == 0;
        });
    }

    @Override
    public String toString() {
        String elemsStr = elems.stream().map(Literal::toString).collect(Collectors.joining(", "));
        return value + " IN (" + elemsStr + ")";
    }
}
