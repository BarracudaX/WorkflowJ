package com.barracuda.engine.event;

public sealed interface ExecutionEvent {

    long flowID();

    record ContinueEvent(long flowID) implements ExecutionEvent{}

    sealed interface FlowEvent extends ExecutionEvent{

        record FlowStartEvent(long flowID) implements FlowEvent { }

        record FlowCompletedEvent(long flowID) implements FlowEvent { }

        record FlowFailedEvent(long flowID, RuntimeException exception) implements FlowEvent { }

        record FlowPausedEvent(long flowID) implements FlowEvent{ }

    }

    sealed interface TaskEvent extends ExecutionEvent{

        long taskID();

        record TaskStartEvent(long flowID, long taskID) implements TaskEvent { }

        record TaskCompletedEvent(long flowID, long taskID) implements TaskEvent { }

        record TaskFailedEvent(long flowID, long taskID, RuntimeException exception) implements TaskEvent { }

        record TaskPausedEvent(long flowID, long taskID) implements TaskEvent { }

    }
}
