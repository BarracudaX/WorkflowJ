package com.barracuda.engine.event;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class InMemoryEventCapturer implements FlowEventListener {

    private final ConcurrentLinkedQueue<FlowEvent> events = new ConcurrentLinkedQueue<>();

    @Override
    public void onEvent(FlowEvent event) {
        events.add(event);
    }

    public List<FlowEvent> events(){
        return events.stream().toList();
    }

}
