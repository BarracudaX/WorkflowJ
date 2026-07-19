package com.barracuda.engine.event;

public sealed interface FlowEvent {

    record FlowStartedEvent(long flowID) implements FlowEvent{

    }

}
