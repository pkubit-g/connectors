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

    /**
     * Returns a List of the children of this node. Children should not change.
     */
    public abstract List<Expression> children();

    /**
     * Checks the input data types, throwing a RuntimeException if invalid.
     * Note: it's not valid to call this method until all children are bound.
     */
    public void verifyInputDataTypes() { }

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
        if (_bound || children().isEmpty()) return true;

        _bound = children().stream().allMatch(Expression::bound);

        if (_bound) {
            // this is the first time that all children are bound, so let's verify their data types
            verifyInputDataTypes();
        }

        return _bound;
    }
}
