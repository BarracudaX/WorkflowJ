package com.barracuda.engine.event;

public interface FlowEventPublisher {

    void publish(FlowEvent event);

    void subscribe(FlowEventListener eventListener);
}
