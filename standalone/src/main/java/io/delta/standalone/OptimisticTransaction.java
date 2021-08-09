package io.delta.standalone;

import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.data.RowRecord;
import io.delta.standalone.operations.Operation;
import io.delta.standalone.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public interface OptimisticTransaction {
    long commit(List<Action> actions, Operation op);

    // TODO should be iter
    List<AddFile> writeFiles(List<RowRecord> data);

    static List<RowRecord> parseData(List<List<Object>> data, StructType schema) {
        String[] schemaFields = schema.getFieldNames();
        List<RowRecord> output = new ArrayList<>();
        return data.stream().map(inputRow -> {
            if (inputRow.size() != schemaFields.length) {
                throw new RuntimeException("data columns do not match expected schema columns");
            }

            RowRecord outputRow = RowRecord.empty(schema);
            for (int i = 0; i < inputRow.size(); i++) {
                outputRow.add(schemaFields[i], inputRow.get(i));
            }

            return outputRow;
        }).collect(Collectors.toList());
    }
}
