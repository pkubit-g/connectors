package io.delta.standalone.expressions;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.DataType;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Usage: new In(expr, literalList) - Returns true if `expr` is equal to any in `expr`, else false.
 */
public final class In extends Predicate {
    private final Expression value;
    private final List<? extends Expression> elems;

    public In(Expression value, List<? extends Expression> elems) {
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

        if (null == result) {
            throw new RuntimeException("'In' expression 'value.eval' result can't be null");
        }
        if (!value.bound()) {
            throw new RuntimeException("'In' expression can't operate on an unbound 'value'");
        }
        if (!elems.get(0).dataType().equals(value.dataType())) {
            throw new IllegalArgumentException("'In' expression 'value' DataType must be the same as 'elems'");
        }

        return elems.stream().anyMatch(setElem -> {
            Object setElemValue = setElem.eval(record);
            if (null == setElemValue) {
                throw new RuntimeException("'In' expression 'elems(i).eval' result can't be null");
            }
            if (!setElem.bound()) {
                throw new RuntimeException("'In' expression can't operate on an unbound 'elems(i)'");
            }

            return Util.compare(value.dataType(), result, setElemValue) == 0;
        });
    }

    @Override
    public String toString() {
        String elemsStr = elems.stream().map(Expression::toString).collect(Collectors.joining(", "));
        return value + " IN (" + elemsStr + ")";
    }

    @Override
    public List<Expression> children() {
        return Stream.concat(Stream.of(value), elems.stream()).collect(Collectors.toList());
    }
}
