package io.delta.standalone.types;

/**
 * A field inside a {@code StructType}.
 */
public final class StructField {
    private final String name;
    private final DataType dataType;
    private final boolean nullable;

    /**
     * @param name  the name of this field
     * @param dataType  the data type of this field
     * @param nullable  indicates if values of this field can be {@code null} values
     */
    public StructField(String name, DataType dataType, boolean nullable) {
        this.name = name;
        this.dataType = dataType;
        this.nullable = nullable;
    }

    /**
     * Constructor with default {@code nullable = true}.
     *
     * @param name  the name of this field
     * @param dataType  the data type of this field
     */
    public StructField(String name, DataType dataType) {
        this(name, dataType, true);
    }

    /**
     * @return the name of this field
     */
    public String getName() {
        return name;
    }

    /**
     * @return the data type of this field
     */
    public DataType getDataType() {
        return dataType;
    }

    /**
     * @return {@code true} if this field as have a {@code null} value, else {@code false}
     */
    public boolean isNullable() {
        return nullable;
    }

    /**
     * Builds a readable {@code String} representation of this {@code StructField}.
     */
    protected void buildFormattedString(String prefix, StringBuilder builder) {
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
