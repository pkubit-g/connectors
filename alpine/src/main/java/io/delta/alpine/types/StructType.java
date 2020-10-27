package io.delta.alpine.types;

import java.util.Arrays;
import java.util.HashMap;

/**
 * The data type representing a table's schema, consisting of a collection of
 * fields (that is, {@code fieldName} to {@code fieldDataType} pairs).
 *
 * @see StructField StructField
 */
public final class StructType extends DataType {
    private final StructField[] fields;
    private final HashMap<String, StructField> nameToField;

    public StructType(StructField[] fields) {
        this.fields = fields;

        // generate name -> field map
        this.nameToField = new HashMap<>();
        Arrays.stream(fields).forEach(field -> nameToField.put(field.getName(), field));
    }

    /**
     * @return array of fields
     */
    public StructField[] getFields() {
        return fields.clone();
    }

    /**
     * @return array of field names
     */
    public String[] getFieldNames() {
        return Arrays.stream(fields).map(StructField::getName).toArray(String[]::new);
    }

    /**
     * @param fieldName  the name of the desired {@code StructField}, not null
     * @return the {@code StructField} with the given name, not null
     * @throws IllegalArgumentException if a field with the given name does not exist
     */
    public StructField get(String fieldName) {
        if (!nameToField.containsKey(fieldName)) {
            throw new IllegalArgumentException(
                String.format(
                        "Field \"%s\" does not exist. Available fields: %s",
                        fieldName,
                        Arrays.toString(getFieldNames()))
                );
        }

        return nameToField.get(fieldName);
    }

    /**
     * Builds a readable indented tree representation of this {@code StructType}
     * and all of its nested elements.
     */
    public String getTreeString() {
        final String prefix = " |";
        StringBuilder builder = new StringBuilder();
        builder.append("root\n");
        Arrays.stream(fields).forEach(field -> field.buildFormattedString(prefix, builder));
        return builder.toString();
    }

    /**
     * Builds a readable {@code String} representation of this {@code StructType}
     * and all of its nested elements.
     */
    protected void buildFormattedString(String prefix, StringBuilder builder) {
        Arrays.stream(fields).forEach(field -> field.buildFormattedString(prefix, builder));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StructType that = (StructType) o;
        return Arrays.equals(fields, that.fields); // TODO: AnyRef?
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(fields); // TODO: AnyRef?
    }
}
