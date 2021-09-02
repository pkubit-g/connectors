package io.delta.standalone.expressions;

import io.delta.standalone.types.DataType;

public interface Expression {
    /**
     * TODO UPDATE COMMENT
     * Returns the result of evaluating this expression on a given input Row
     */
    Expression eval();

    /**
     * TODO UPDATE COMMENT
     * Returns the [[DataType]] of the result of evaluating this expression.  It is
     * invalid to query the dataType of an unresolved expression (i.e., when `resolved` == false).
     */
    DataType dataType();

    /**
     * TODO UPDATE COMMENT
     * @return a readable indented tree representation of this {@code Expression}
     *         and all of its nested {@code Expression}s
     */
    String treeString();
}
