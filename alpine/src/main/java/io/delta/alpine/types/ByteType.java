package io.delta.alpine.types;

/**
 * The data type representing {@code byte} values.
 */
public final class ByteType extends DataType {
    @Override
    public String getSimpleString() {
        return "tinyint";
    }
}
