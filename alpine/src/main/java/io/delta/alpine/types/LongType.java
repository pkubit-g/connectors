package io.delta.alpine.types;

/**
 * The data type representing `Long` values.
 */
public class LongType extends DataType {
    @Override
    public String getSimpleString() {
        return "bigint";
    }
}
