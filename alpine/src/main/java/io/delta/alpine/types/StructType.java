package io.delta.alpine.types;

import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.Collectors;

public class StructType extends DataType {
    private StructField[] fields; // TODO: make this a List?
    private HashMap<String, StructField> nameToField;

    public StructType(StructField[] fields) {
        this.fields = fields;
        this.nameToField = null;
    }

    public StructField[] getFields() {
        return fields;
    }

    public String[] getFieldNames() {
        return Arrays.stream(fields).map(StructField::getName).toArray(String[]::new);
    }

    public StructField getDataTypeForField(String fieldName) {
        if (null == nameToField) {
            generateNameToFieldMap();
        }

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

    private void generateNameToFieldMap() {
        nameToField = new HashMap<>();
        Arrays.stream(fields).forEach(field -> nameToField.put(field.getName(), field));
    }
}
