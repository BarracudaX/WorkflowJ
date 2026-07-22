package com.barracuda.engine.event;


public sealed interface ExecutionEvent {

    sealed interface CommandEvent extends ExecutionEvent {

        record Continue() implements CommandEvent{}

        record Reset() implements CommandEvent{}
    }

    sealed interface SubflowEvent extends ExecutionEvent{
        long rootID();

        long subflowID();

        record SubflowStartedEvent(long rootID, long subflowID) implements SubflowEvent{ }

        record SubflowCompletedEvent(long rootID, long subflowID) implements SubflowEvent{ }

        record SubflowFailedEvent(long rootID, long subflowID, RuntimeException exception) implements SubflowEvent{ }

        record SubflowPausedEvent(long rootID, long subflowID) implements SubflowEvent{ }
    }

    sealed interface FlowEvent extends ExecutionEvent{
        long flowID();

        record FlowStartedEvent(long flowID) implements FlowEvent { }

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
