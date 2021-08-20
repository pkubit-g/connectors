package io.delta.standalone;

import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.data.RowRecord;
import io.delta.standalone.operations.Operation;

import java.util.List;

public interface OptimisticTransaction {
    long commit(List<Action> actions, Operation op);

    long commit(List<Action> actions, Operation op, CommitConflictChecker commitConflictChecker);

    // TODO should be iter?
    long writeRecordsAndCommit(List<RowRecord> data);

    void addReadFiles(Iterable<AddFile> readFiles);

    /**
     * Configure this transaction to be a blind append. By default, it is not a blind append and
     * assumes you have read some part of the table.
     */
    void setIsBlindAppend();
}
