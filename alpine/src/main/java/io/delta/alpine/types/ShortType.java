package io.delta.alpine.types;

/**
 * The data type representing `Short` values.
 */
public final class ShortType extends DataType {
    @Override
    public String getSimpleString() {
        return "smallint";
    }
}
