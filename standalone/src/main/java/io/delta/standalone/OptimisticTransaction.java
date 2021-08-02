package io.delta.standalone;

import io.delta.standalone.actions.Action;
import io.delta.standalone.operations.Operation;

import java.util.List;

public interface OptimisticTransaction {
    long commit(List<Action> actions);
    long commit(List<Action> actions, Operation op);
}
