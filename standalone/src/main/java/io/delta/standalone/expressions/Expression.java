package io.delta.standalone.expressions;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.DataType;

import java.util.List;

/**
 * An expression in Delta Standalone.
 */
public abstract class Expression {
    /**
     * Short-circuit for bound() calculation. Once set to true, will remain true.
     */
    private boolean _bound = false;
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

    public abstract List<Expression> children();

    /**
     * Returns `true` if this expression and all its children have been bound to a specific schema
     * and input data types checking passed, and `false` if it still contains any unresolved
     * placeholders or has data types mismatch.
     *
     * Implementations of expressions should override this if the resolution of this type of
     * expression involves more than just the resolution of its children and type checking.
     *
     * For example:
     * - a Column that hasn't been evaluated yet will return false.
     * - a Column that has been evaluated will return true.
     * - a Literal will return true
     */
    boolean bound() {
        if (children().isEmpty()) return true;
        if (_bound) return true;

        DataType firstChildDataType = children().get(0).dataType();
        _bound = children().stream().allMatch(child ->
            child.bound() && child.dataType().equals(firstChildDataType)
        );

        return _bound;
    }
}
