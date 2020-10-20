package io.delta.alpine.types;

/**
 * The data type representing `Int` values.
 */
public final class IntegerType extends DataType {
    @Override
    public String getSimpleString() {
        return "int";
    }
}
