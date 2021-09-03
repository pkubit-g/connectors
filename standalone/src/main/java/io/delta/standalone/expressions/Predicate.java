package io.delta.standalone.expressions;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.DataType;

/**
 * An [[Expression]] that returns a boolean value.
 */
public interface Predicate extends Expression {
    @Override
    default DataType dataType() {
        return new BooleanType();
    }

    @Override
    Boolean eval(RowRecord record);
}
