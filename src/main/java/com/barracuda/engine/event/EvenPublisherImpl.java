package com.barracuda.engine.event;


import java.util.concurrent.CopyOnWriteArrayList;

public final class EvenPublisherImpl implements FlowEventPublisher {

    private final CopyOnWriteArrayList<FlowEventListener> listeners = new CopyOnWriteArrayList<>();

    @Override
    public void subscribe(FlowEventListener listener) {
        listeners.add(listener);
    }

    @Override
    public void publish(FlowEvent event) {
        listeners.forEach(listener -> listener.onEvent(event));
    }
}
