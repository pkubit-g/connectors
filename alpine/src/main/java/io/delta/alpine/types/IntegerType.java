package io.delta.alpine.types;

/**
 * The data type representing `Int` values.
 */
public class IntegerType extends DataType {
    @Override
    public String getSimpleString() {
        return "int";
    }
}
