package io.delta.alpine.types;

/**
 * The data type representing `Byte` values.
 */
public class ByteType extends DataType {
    @Override
    public String getSimpleString() {
        return "tinyint";
    }
}
