package io.delta.standalone;

import io.delta.standalone.operations.Operation;

import java.util.List;

class Action {}

public interface OptimisticTransaction {
    long commit(List<Action> actions);
    long commit(List<Action> actions, Operation op);
}
