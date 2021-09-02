package io.delta.standalone.expressions;

import io.delta.standalone.types.DataType;

public class Column implements Expression {
    private final String name;

    public Column(String name) {
        this.name = name;
    }

    public String name() {
        return name;
    }

    @Override
    public Expression eval() {
        throw new UnsupportedOperationException("TODO");
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
