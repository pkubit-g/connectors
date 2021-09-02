package io.delta.standalone.expressions;

import io.delta.standalone.types.BooleanType;

public class BooleanLiteral extends Literal {
    public static BooleanLiteral True = new BooleanLiteral(true);
    public static BooleanLiteral False = new BooleanLiteral(false);

    private final boolean _value;

    public BooleanLiteral(boolean value) {
        super(value, new BooleanType());

        this._value = value;
    }

    @Override
    public Boolean value() {
        return _value;
    }
}
