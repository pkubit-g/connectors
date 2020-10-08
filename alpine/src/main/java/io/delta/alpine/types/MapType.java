package io.delta.alpine.types;

/**
 * The data type for Maps. Keys in a map are not allowed to have `null` values.
 */
public class MapType extends DataType {
    private DataType keyType;
    private DataType valueType;
    private boolean valueContainsNull;

    /**
     * @param keyType The data type of map keys.
     * @param valueType The data type of map values.
     * @param valueContainsNull Indicates if map values have `null` values.
     */
    public MapType(DataType keyType, DataType valueType, boolean valueContainsNull) {
        this.keyType = keyType;
        this.valueType = valueType;
        this.valueContainsNull = valueContainsNull;
    }

    public DataType getKeyType() {
        return keyType;
    }

    public DataType getValueType() {
        return valueType;
    }

    public boolean valueContainsNull() {
        return valueContainsNull;
    }

    public void buildFormattedString(String prefix, StringBuilder builder) {
        final String nextPrefix = prefix + "    |";
        builder.append(String.format("%s-- key: %s\n", prefix, keyType.getTypeName()));
        DataType.buildFormattedString(keyType, nextPrefix, builder);
        builder.append(String.format("%s-- value: %s (valueContainsNull = %b)\n", prefix, valueType.getTypeName(), valueContainsNull));
    }
}
