package io.delta.standalone.expressions;

import io.delta.standalone.types.DataType;
import io.delta.standalone.types.StructType;

import java.util.Map;

public class PartitionFilterEvaluator extends ResolvingEvaluator {
    private Map<String, String> currentExprPartitionValues;

    public PartitionFilterEvaluator(StructType partitionSchema) {
        super(partitionSchema);
    }

    public boolean eval(Expression root, Map<String, String> partitionValues) {
        currentExprPartitionValues = partitionValues;
        Expression result = _eval(root);

        if (!(result instanceof BooleanLiteral)) {
            throw new RuntimeException("filter expr result must be a BooleanLiteral");
        }

        return ((BooleanLiteral) result).value();
    }

    private Expression _eval(Expression expr) {
        if (expr instanceof Column) {
            Column column = (Column) expr;
            DataType dataType = columnTypes.get(column.name());
            String value = currentExprPartitionValues.get(column.name());
            return Literal.fromString(value, dataType);
        }

        return _eval(expr.eval());
    }
}
