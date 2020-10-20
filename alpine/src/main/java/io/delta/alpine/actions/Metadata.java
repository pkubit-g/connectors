package io.delta.alpine.actions;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import io.delta.alpine.types.StructType;

public final class Metadata {
    private final String id;
    private final String name;
    private final String description;
    private final Format format;
    private final String schemaString;
    private final List<String> partitionColumns;
    private final Optional<Long> createdTime;
    private final StructType schema;

    public Metadata(String id, String name, String description, Format format, String schemaString,
                    List<String> partitionColumns, Optional<Long> createdTime, StructType schema) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.format = format;
        this.schemaString = schemaString;
        this.partitionColumns = partitionColumns;
        this.createdTime = createdTime;
        this.schema = schema;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public Format getFormat() {
        return format;
    }

    public String getSchemaString() {
        return schemaString;
    }

    public List<String> getPartitionColumns() {
        return Collections.unmodifiableList(partitionColumns);
    }

    public Optional<Long> getCreatedTime() {
        return createdTime;
    }

    public StructType getSchema() {
        return schema;
    }
}
