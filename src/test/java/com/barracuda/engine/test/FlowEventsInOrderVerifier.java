package com.barracuda.engine.test;

import com.barracuda.engine.event.ExecutionEvent.FlowEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowCompletedEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowStartEvent;
import com.barracuda.engine.flow.Flow;
import org.assertj.core.api.InstanceOfAssertFactories;

import java.util.ArrayList;
import java.util.List;

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

    public FlowEventsInOrderVerifier hasFlowStartedEvent() {
        var remainingEvents = List.copyOf(events);
        FlowEvent nextEvent = getNextEvent();
        assertThat(nextEvent)
                .withFailMessage("Expected FlowStartedEvent, but was "+nextEvent+". All remaining events: "+remainingEvents)
                .isInstanceOf(FlowStartEvent.class)
                .satisfies(event -> assertThat(event.flowID()).isEqualTo(flow.id()));
        return this;
    }

    public FlowEventsInOrderVerifier hasFlowPausedEvent() {
        var remainingEvents = List.copyOf(events);
        FlowEvent nextEvent = getNextEvent();
        assertThat(nextEvent)
                .withFailMessage("Expected FlowPausedEvent, but was "+nextEvent+". All remaining events: "+remainingEvents)
                .asInstanceOf(type(FlowEvent.FlowPausedEvent.class))
                .satisfies(event -> assertThat(event.flowID()).isEqualTo(flow.id()));

        return this;
    }

    public FlowEventsInOrderVerifier hasFlowFailedEvent(RuntimeException exception) {
        var remainingEvents = List.copyOf(events);
        FlowEvent nextEvent = getNextEvent();
        assertThat(nextEvent)
                .withFailMessage("Expected FlowFailedEvent, but was "+nextEvent+". All remaining events: "+remainingEvents)
                .asInstanceOf(InstanceOfAssertFactories.type(FlowEvent.FlowFailedEvent.class))
                .satisfies(event -> assertThat(event.flowID()).isEqualTo(flow.id()))
                .satisfies(event -> assertThat(event.exception()).isEqualTo(exception));
        return this;
    }

    public FlowEventsInOrderVerifier hasFlowCompletedEvent() {
        var remainingEvents = List.copyOf(events);
        FlowEvent nextEvent = getNextEvent();
        assertThat(nextEvent)
                .withFailMessage("Expected FlowCompletedEvent, but was "+nextEvent+". All remaining events: "+remainingEvents)
                .isInstanceOf(FlowCompletedEvent.class)
                .satisfies(event -> assertThat(event.flowID()).isEqualTo(flow.id()));

        return this;
    }

    private FlowEvent getNextEvent(){
        if(events.isEmpty()) {
            throw new IllegalStateException("No events left");
        }
        return events.removeFirst();
    }


}
