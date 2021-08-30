package io.delta.standalone;

import io.delta.standalone.actions.AddFile;

import java.util.List;

@FunctionalInterface
public interface AddedFilesConflictChecker {
    boolean shouldHaveRead(List<AddFile> otherCommitAddedFiles);
}
