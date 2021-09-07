package io.delta.standalone.expressions;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.*;

import java.util.Arrays;
import java.util.Optional;

/**
 * A column whose row-value will be computed based on the data in a [[RowRecord]].
 *
 * Usage: new Column(columnName).
 */
public final class Column extends LeafExpression {
    private final String name;
    private Optional<DataType> dataTypeOpt;

    public Column(String name) {
        this.name = name;
        this.dataTypeOpt = Optional.empty();
    }

    public String name() {
        return name;
    }

    @Override
    public Object eval(RowRecord record) {
        Optional<StructField> matchingField = Arrays.stream(record.getSchema().getFields())
            .filter(x -> x.getName().equals(name))
            .findFirst();

        if (!matchingField.isPresent()) {
            String fieldsStr = String.join(",", record.getSchema().getFieldNames());
            throw new RuntimeException("Column '" + name + "' doesn't exist in RowRecord Schema: " + fieldsStr);
        }

        DataType columnDataType = matchingField.get().getDataType();

        if (columnDataType instanceof IntegerType) {
            dataTypeOpt = Optional.of(new IntegerType());
            return record.getInt(name);
        }
        if (columnDataType instanceof BooleanType) {
            dataTypeOpt = Optional.of(new BooleanType());
            return record.getBoolean(name);
        }

        throw new RuntimeException("Couldn't find matching rowRecord DataType for column: " + name);
    }

    @Override
    public boolean bound() {
        return dataTypeOpt.isPresent();
    }

    @Override
    public DataType dataType() {
        if (dataTypeOpt.isPresent()) return dataTypeOpt.get();

        throw new RuntimeException("Can't call 'dataType' on a unevaluated Column");
    }

    @Override
    public String toString() {
        return "Column(" + name + ")";
    }
}
