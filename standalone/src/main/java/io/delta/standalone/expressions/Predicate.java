package io.delta.standalone.expressions;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.DataType;

/**
 * An [[Expression]] that returns a boolean value.
 */
public abstract class Predicate extends Expression {
    @Override
    public DataType dataType() {
        return new BooleanType();
    }

    @Override
    public abstract Boolean eval(RowRecord record);
}
