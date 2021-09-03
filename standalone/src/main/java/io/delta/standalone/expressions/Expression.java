package io.delta.standalone.expressions;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.DataType;

/**
 * An expression in Delta Standalone.
 */
public interface Expression {
    /**
     * Returns the result of evaluating this expression on a given input RowRecord.
     */
    Object eval(RowRecord record);

    /**
     * Returns the [[DataType]] of the result of evaluating this expression.
     */
    DataType dataType();

    /**
     * Returns the String representation of this expression.
     */
    String toString();
}
