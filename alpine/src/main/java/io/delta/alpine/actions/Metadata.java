package io.delta.alpine.actions;

import java.util.List;
import java.util.Optional;

import io.delta.alpine.types.StructType;

public class Metadata {
    private String id;
    private String name;
    private String description;
    private Format format;
    private String schemaString;
    private List<String> partitionColumns;
    private Optional<Long> createdTime;
    private StructType schema;

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
        return partitionColumns;
    }

    public Optional<Long> getCreatedTime() {
        return createdTime;
    }

    public StructType getSchema() {
        return schema;
    }
}
