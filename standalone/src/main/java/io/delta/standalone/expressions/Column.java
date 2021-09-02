package io.delta.standalone.expressions;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.*;

import java.util.Arrays;
import java.util.Optional;

public class Column implements Expression {
    private final String name;

    public Column(String name) {
        this.name = name;
    }

    public String name() {
        return name;
    }

    @Override
    public Object eval(RowRecord record) {
        Optional<StructField> matchingField = Arrays.stream(record.getSchema().getFields())
            .filter(x -> x.getName() == name)
            .findFirst();

        if (!matchingField.isPresent()) {
            throw new RuntimeException("Column '" + name + "' doesn't exist in RowRecord");
        }
        DataType columnDataType = matchingField.get().getDataType();

        if (columnDataType instanceof IntegerType) return record.getInt(name);
        if (columnDataType instanceof BooleanType) return record.getBoolean(name);

        throw new RuntimeException("Couldn't find matching rowRecord DataType for column:" + name);
    }

    @Override
    public DataType dataType() {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public String treeString() {
        return "Column(" + name + ")";
    }
}
