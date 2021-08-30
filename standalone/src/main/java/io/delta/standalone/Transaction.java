package io.delta.standalone;

import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.data.RowRecord;

import java.util.List;

public interface Transaction {
    void addReadFiles(Iterable<AddFile> readFiles);

    CommitResult commit(Iterable<Action> actions, Operation op);

    CommitResult commit(
        Iterable<Action> actions,
        Operation op,
        AddedFilesConflictChecker addedFilesConflictChecker);

    void updateMetadata(Metadata metadata);

    List<AddFile> writeData(Iterable<RowRecord> data);
}
