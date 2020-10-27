package io.delta.alpine.types;

/**
 * The data type for collections of multiple values.
 */
public final class ArrayType extends DataType {
    private final DataType elementType;
    private final boolean containsNull;

    /**
     * @param elementType  the data type of values
     * @param containsNull  indicates if values have {@code null} value
     */
    public ArrayType(DataType elementType, boolean containsNull) {
        this.elementType = elementType;
        this.containsNull = containsNull;
    }

    /**
     * @return the type of array elements
     */
    public DataType getElementType() {
        return elementType;
    }

    /**
     * @return {@code true} if the array has {@code null} values, else {@code false}
     */
    public boolean containsNull() {
        return containsNull;
    }

    /**
     * Builds a readable {@code String} representation of this {@code ArrayType}.
     */
    protected void buildFormattedString(String prefix, StringBuilder builder) {
        final String nextPrefix = prefix + "    |";
        builder.append(String.format("%s-- element: %s (containsNull = %b)\n", prefix, elementType.getTypeName(), containsNull));
        DataType.buildFormattedString(elementType, nextPrefix, builder);
    }
}
