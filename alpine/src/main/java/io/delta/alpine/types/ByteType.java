package io.delta.alpine.types;

/**
 * The data type representing `Byte` values.
 */
public final class ByteType extends DataType {
    @Override
    public String getSimpleString() {
        return "tinyint";
    }
}
