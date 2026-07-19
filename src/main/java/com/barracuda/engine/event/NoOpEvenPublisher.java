package com.barracuda.engine.event;

public class NoOpEvenPublisher implements FlowEventPublisher {

    @Override
    public void publish(ExecutionEvent event) {

    }

    @Override
    public void subscribe(FlowEventListener eventListener) {

    }

}
