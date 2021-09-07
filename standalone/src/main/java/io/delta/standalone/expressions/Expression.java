package io.delta.standalone.expressions;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.DataType;

import java.util.List;

/**
 * An expression in Delta Standalone.
 */
public abstract class Expression {

    /**
     * Returns the result of evaluating this expression on a given input RowRecord.
     */
    public abstract Object eval(RowRecord record);

    /**
     * Returns the [[DataType]] of the result of evaluating this expression.
     */
    public abstract DataType dataType();

    /**
     * Returns the String representation of this expression.
     */
    public abstract String toString();

    /**
     * Returns a List of the children of this node. Children should not change.
     */
    public abstract List<Expression> children();
}
