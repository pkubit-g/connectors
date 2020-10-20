package io.delta.alpine.types;

import java.util.Arrays;
import java.util.HashMap;

public final class StructType extends DataType {
    private final StructField[] fields;
    private final HashMap<String, StructField> nameToField;

    public StructType(StructField[] fields) {
        this.fields = fields;

        // generate name -> field map
        this.nameToField = new HashMap<>();
        Arrays.stream(fields).forEach(field -> nameToField.put(field.getName(), field));
    }

    public StructField[] getFields() {
        return fields.clone();
    }

    public String[] getFieldNames() {
        return Arrays.stream(fields).map(StructField::getName).toArray(String[]::new);
    }

    public StructField get(String fieldName) {
        return nameToField.get(fieldName);
    }

    public String getTreeString() {
        final String prefix = " |";
        StringBuilder builder = new StringBuilder();
        builder.append("root\n");
        Arrays.stream(fields).forEach(field -> field.buildFormattedString(prefix, builder));
        return builder.toString();
    }

    public void buildFormattedString(String prefix, StringBuilder builder) {
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
