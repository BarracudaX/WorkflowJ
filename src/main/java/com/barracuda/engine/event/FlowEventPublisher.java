package com.barracuda.engine.event;

public interface FlowEventPublisher {

    void publish(ExecutionEvent event);

    void subscribe(FlowEventListener eventListener);
}
