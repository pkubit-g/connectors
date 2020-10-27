package io.delta.standalone.types;

/**
 * The data type representing {@code int} values.
 */
public final class IntegerType extends DataType {
    @Override
    public String getSimpleString() {
        return "int";
    }
}
