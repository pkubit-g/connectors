package io.delta.alpine.types;

/**
 * A field inside a StructType.
 */
public class StructField{
    private String name;
    private DataType dataType;
    private boolean nullable;

    /**
     * @param name The name of this field.
     * @param dataType The data type of this field.
     * @param nullable Indicates if values of this field can be `null` values.
     */
    public StructField(String name, DataType dataType, boolean nullable) {
        this.name = name;
        this.dataType = dataType;
        this.nullable = nullable;
    }

    public StructField(String name, DataType dataType) {
        this(name, dataType, true);
    }

    public String getName() {
        return name;
    }

    public DataType getDataType() {
        return dataType;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void buildFormattedString(String prefix, StringBuilder builder) {
        final String nextPrefix = prefix + "    |";
        builder.append(String.format("%s-- %s: %s (nullable = %b)\n", prefix, name, dataType.getTypeName(), nullable));
        DataType.buildFormattedString(dataType, nextPrefix, builder);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StructField that = (StructField) o;
        return name.equals(that.name) && dataType.equals(that.dataType) && nullable == that.nullable;
    }
}
