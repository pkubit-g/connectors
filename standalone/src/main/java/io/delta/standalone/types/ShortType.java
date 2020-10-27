package io.delta.standalone.types;

/**
 * The data type representing {@code short} values.
 */
public final class ShortType extends DataType {
    @Override
    public String getSimpleString() {
        return "smallint";
    }
}
