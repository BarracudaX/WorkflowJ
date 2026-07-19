package com.barracuda.engine.event;

public sealed interface ExecutionEvent {

    sealed interface FlowEvent extends ExecutionEvent{

        long flowID();

        record FlowStartedEvent(long flowID) implements FlowEvent { }

        record FlowCompletedEvent(long flowID) implements FlowEvent { }

        record FlowFailedEvent(long flowID, Exception exception) implements FlowEvent { }

        record FlowPausedEvent(long flowID) implements FlowEvent{ }

    }

    sealed interface TaskEvent extends ExecutionEvent{

        long taskID();

        record TaskStartedEvent(long taskID) implements TaskEvent { }

        record TaskCompletedEvent(long taskID) implements TaskEvent { }

        record TaskFailedEvent(long taskID, Exception exception) implements TaskEvent { }

        record TaskPausedEvent(long taskID) implements TaskEvent { }

    }
}
