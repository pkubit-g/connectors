package io.delta.standalone.expressions;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.DataType;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * An expression in Delta Standalone.
 */
public interface Expression {

    /**
     * @return the result of evaluating this expression on a given input RowRecord.
     */
    Object eval(RowRecord record);

    /**
     * @return the [[DataType]] of the result of evaluating this expression.
     */
    DataType dataType();

    /**
     * @return the String representation of this expression.
     */
    String toString();

    default Set<String> references() {
        Set<String> result = new HashSet<>();
        children().forEach(child -> result.addAll(child.references()));
        return result;
    }

    /**
     * @return a List of the children of this node. Children should not change.
     */
    List<Expression> children();
}
