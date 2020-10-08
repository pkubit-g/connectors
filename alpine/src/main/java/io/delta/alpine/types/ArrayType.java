package io.delta.alpine.types;

/**
 * The data type for collections of multiple values.
 * Internally these are represented as columns that contain a ``scala.collection.Seq``.
 *
 * An [[ArrayType]] object comprises two fields, `elementType: [[DataType]]` and
 * `containsNull: Boolean`. The field of `elementType` is used to specify the type of
 * array elements. The field of `containsNull` is used to specify if the array has `null` values.
 */
public class ArrayType extends DataType {
    private DataType elementType;
    private boolean containsNull;

    /**
     *
     * @param elementType The data type of values.
     * @param containsNull Indicates if values have `null` values
     */
    public ArrayType(DataType elementType, boolean containsNull) {
        this.elementType = elementType;
        this.containsNull = containsNull;
    }

    public DataType getElementType() {
        return elementType;
    }

    public boolean containsNull() {
        return containsNull;
    }

    public void buildFormattedString(String prefix, StringBuilder builder) {
        final String nextPrefix = prefix + "    |";
        builder.append(String.format("%s-- element: %s (containsNull = %b)\n", prefix, elementType.getTypeName(), containsNull));
        DataType.buildFormattedString(elementType, nextPrefix, builder);
    }
}
