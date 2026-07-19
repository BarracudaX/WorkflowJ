package com.barracuda.engine.event;

public sealed interface FlowEvent {

    record FlowStartedEvent(long flowID) implements FlowEvent{ }

    record TaskStartedEvent(long taskID) implements FlowEvent{ }

    record TaskCompletedEvent(long taskID) implements FlowEvent{ }

    record TaskFailedEvent(long taskID,Exception exception) implements FlowEvent{ }
}
