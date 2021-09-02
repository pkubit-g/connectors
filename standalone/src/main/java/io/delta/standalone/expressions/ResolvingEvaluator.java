package io.delta.standalone.expressions;

import io.delta.standalone.types.DataType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class ResolvingEvaluator {
    protected final Map<String, DataType> columnTypes;

    public ResolvingEvaluator(StructType partitionSchema) {
        this.columnTypes = Arrays
            .stream(partitionSchema.getFields())
            .collect(Collectors.toMap(StructField::getName, StructField::getDataType));
    }
}
