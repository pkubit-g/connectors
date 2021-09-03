package io.delta.standalone.expressions;

import java.util.Collections;
import java.util.List;

public abstract class LeafExpression extends Expression {
    @Override
    public List<Expression> children() {
        return Collections.emptyList();
    }
}
