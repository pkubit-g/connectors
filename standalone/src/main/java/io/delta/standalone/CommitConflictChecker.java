package io.delta.standalone;

import io.delta.standalone.actions.AddFile;

import java.util.List;

@FunctionalInterface
public interface CommitConflictChecker {
    boolean doesConflict(List<AddFile> otherCommitFiles);
}
