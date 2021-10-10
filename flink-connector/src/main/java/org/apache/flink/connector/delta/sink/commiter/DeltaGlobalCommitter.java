package org.apache.flink.connector.delta.sink.commiter;

import io.delta.standalone.CommitResult;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.connector.delta.sink.committables.DeltaCommittable;
import org.apache.flink.connector.delta.sink.committables.DeltaGlobalCommittable;
import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaPendingFile;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeltaGlobalCommitter implements GlobalCommitter<DeltaCommittable, DeltaGlobalCommittable> {

    private final String engineInfo = "flink.1.12"; // TODO parametrize

    private final Configuration conf;

    private final Path basePath;

    public DeltaGlobalCommitter(Configuration conf,
                                Path basePath) {
        this.conf = conf;
        this.basePath = basePath;
    }


    @Override
    public List<DeltaGlobalCommittable> filterRecoveredCommittables(List<DeltaGlobalCommittable> globalCommittables) throws IOException {
        return globalCommittables;
    }

    @Override
    public DeltaGlobalCommittable combine(List<DeltaCommittable> committables) throws IOException {
        List<DeltaPendingFile> pendingFiles = new ArrayList<>();
        for (DeltaCommittable committable : committables) {
            if (committable.hasDeltaPendingFile()) {
                assert committable.getDeltaPendingFile() != null;
                assert committable.getDeltaPendingFile().getFileName() != null;
                pendingFiles.add(committable.getDeltaPendingFile());
            }
        }
        return new DeltaGlobalCommittable(pendingFiles);
    }

    @Override
    public List<DeltaGlobalCommittable> commit(List<DeltaGlobalCommittable> globalCommittables) throws IOException {
        if (!globalCommittables.isEmpty()) {
            List<Action> addFileActions = prepareAddFileActions(globalCommittables);

            Operation operation = prepareOperation(globalCommittables, addFileActions.size());
            DeltaLog deltaLog = DeltaLog.forTable(conf, basePath.getPath());

            Metadata metadata = deltaLog.snapshot().getMetadata();

            OptimisticTransaction transaction = deltaLog.startTransaction();

            CommitResult result = transaction.commit(addFileActions, operation, this.engineInfo);

        }
        return Collections.emptyList();
    }

    private Operation prepareOperation(List<DeltaGlobalCommittable> globalCommittables,
                                       int numAddedFiles) {
        Map<String, String> operationMetrics = prepareOperationMetrics(globalCommittables, numAddedFiles);
        Map<String, Object> operationParameters = Collections.emptyMap(); // TODO
        return new Operation(Operation.Name.STREAMING_UPDATE, operationParameters, operationMetrics);
    }

    private List<Action> prepareAddFileActions(List<DeltaGlobalCommittable> globalCommittables) {
        List<Action> actions = new ArrayList<>();
        for (DeltaGlobalCommittable globalCommittable : globalCommittables) {
            for (DeltaPendingFile deltaPendingFile : globalCommittable.getPendingFiles()) {
                Map<String, String> partitionValues = Collections.emptyMap(); // TODO
                long modificationTime = System.currentTimeMillis(); // TODO
                Action action = new AddFile(deltaPendingFile.getFileName(), partitionValues, deltaPendingFile.getFileSize(), modificationTime, true, null, null);
                actions.add(action);
            }
        }
        return actions;
    }

    private Map<String, String> prepareOperationMetrics(List<DeltaGlobalCommittable> globalCommittables,
                                                        int numAddedFiles) {
        long cumulatedRecordCount = 0;
        long cumulatedSize = 0;
        for (DeltaGlobalCommittable globalCommittable : globalCommittables) {
            for (DeltaPendingFile deltaPendingFile : globalCommittable.getPendingFiles()) {
                cumulatedRecordCount += deltaPendingFile.getRecordCount();
                cumulatedSize += deltaPendingFile.getFileSize();
            }
        }

        Map<String, String> operationMetrics = new HashMap<>();
        operationMetrics.put("numRemovedFiles", "0"); // TODO
        operationMetrics.put("numAddedFiles", String.valueOf(numAddedFiles));
        operationMetrics.put("numOutputRows", String.valueOf(cumulatedRecordCount));
        operationMetrics.put("numOutputBytes", String.valueOf(cumulatedSize));

        return operationMetrics;
    }

    @Override
    public void endOfInput() throws IOException {

    }

    @Override
    public void close() throws Exception {

    }

    public Configuration getConf() {
        return conf;
    }

    public Path getBasePath() {
        return basePath;
    }


}
