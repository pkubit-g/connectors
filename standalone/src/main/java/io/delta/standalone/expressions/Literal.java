package io.delta.standalone.expressions;

import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.DataType;

public class Literal implements Expression {
    private final Object value;
    private final DataType dataType;

    public Literal(Object value, DataType dataType) {
        Literal.validateLiteralValue(value, dataType);

        this.value = value;
        this.dataType = dataType;
    }

    public Object value() {
        return value;
    }

    @Override
    public Expression eval() {
        return this;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public String treeString() {
        return "Literal(" + value.toString() + ")";
    }

    public static Literal fromString(String str, DataType dataType) {
        if (dataType instanceof BooleanType) new Literal(Boolean.parseBoolean(str), dataType);
        // TODO
        return null;
    }

    private static void validateLiteralValue(Object value, DataType dataType) {
        // TODO
    }

    // TODO hash and equals
}
