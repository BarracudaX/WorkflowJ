package com.barracuda.engine.event;

public sealed interface ExecutionEvent {

    long flowID();

    record ContinueEvent(long flowID) implements ExecutionEvent{}

    sealed interface SubflowEvent extends ExecutionEvent{

        long subflowID();

        record SubflowStartedEvent(long flowID,long subflowID) implements SubflowEvent{ }

        record SubflowCompletedEvent(long flowID,long subflowID) implements SubflowEvent{ }

        record SubflowFailedEvent(long flowID,long subflowID,RuntimeException exception) implements SubflowEvent{ }

        record SubflowPausedEvent(long flowID,long subflowID) implements SubflowEvent{ }
    }

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
