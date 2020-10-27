package io.delta.alpine.types;

/**
 * The data type representing {@code long} values.
 */
public final class LongType extends DataType {
    @Override
    public String getSimpleString() {
        return "bigint";
    }
}
