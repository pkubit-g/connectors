package io.delta.standalone;

import io.delta.standalone.actions.Action;
import io.delta.standalone.data.RowRecord;
import io.delta.standalone.operations.Operation;

import java.util.List;

public interface OptimisticTransaction {
    long commit(List<Action> actions, Operation op);

    // TODO should be iter?
    long writeRecordsAndCommit(List<RowRecord> data);

    // TODO should be iter?
    long writeDataAndCommit(List<List<Object>> data);
}
