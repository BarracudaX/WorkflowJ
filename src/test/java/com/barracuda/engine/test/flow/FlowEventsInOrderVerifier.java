package com.barracuda.engine.test.flow;

import com.barracuda.engine.event.ExecutionEvent.FlowEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowCompletedEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowFailedEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowStartedEvent;
import com.barracuda.engine.flow.Flow;
import org.assertj.core.api.InstanceOfAssertFactories;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

public class FlowEventsInOrderVerifier {

    private final Flow flow;
    private final List<FlowEvent> events = new ArrayList<>();

    FlowEventsInOrderVerifier(Flow flow, List<FlowEvent> events) {
        this.flow = flow;
        this.events.addAll(events);
    }

    public void andHasNoMoreEvents() {
        assertThat(events).isEmpty();
    }

    public FlowEventsInOrderVerifier hasFlowStartedEvent(Consumer<FlowStartedEvent> eventConsumer) {
        eventConsumer.accept(getNextEvent());
        return this;
    }

    public FlowEventsInOrderVerifier hasFlowStartedEvent() {
       return hasFlowStartedEvent(_ -> {});
    }

    public FlowEventsInOrderVerifier hasFlowPausedEvent(Consumer<FlowEvent.FlowPausedEvent> eventConsumer) {
        eventConsumer.accept(getNextEvent());
        return this;
    }

    public FlowEventsInOrderVerifier hasFlowPausedEvent() {
        return hasFlowPausedEvent(_ -> {});
    }

    public FlowEventsInOrderVerifier hasFlowFailedEvent(Consumer<FlowFailedEvent> eventConsumer) {
        eventConsumer.accept(getNextEvent());
        return this;
    }

    public FlowEventsInOrderVerifier hasFlowFailedEvent() {
        return hasFlowFailedEvent(_ -> {});
    }

    public FlowEventsInOrderVerifier hasFlowCompletedEvent(Consumer<FlowCompletedEvent> eventConsumer) {
        eventConsumer.accept(getNextEvent());
        return this;
    }

    public FlowEventsInOrderVerifier hasFlowCompletedEvent() {
        return hasFlowCompletedEvent(_ -> {});
    }

    private <T extends FlowEvent> T getNextEvent(){
        if(events.isEmpty()) {
            throw new IllegalStateException("No events left");
        }
        return (T) events.removeFirst();
    }


}
