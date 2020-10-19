package io.delta.alpine.types;

/**
 * The data type representing `Long` values.
 */
public final class LongType extends DataType {
    @Override
    public String getSimpleString() {
        return "bigint";
    }
}
