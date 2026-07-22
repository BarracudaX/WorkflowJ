package com.barracuda.engine.test;

import com.barracuda.engine.event.ExecutionEvent.SubflowEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.SubflowCompletedEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.SubflowFailedEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.SubflowPausedEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.SubflowStartedEvent;
import com.barracuda.engine.flow.Flow;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

public class SubflowEventsInOrderVerifier {

    private final Flow root;
    private final long subflowID;
    private final List<SubflowEvent> events = new ArrayList<>();

    public SubflowEventsInOrderVerifier(Flow root, List<SubflowEvent> events, long subflowID) {
        this.root = root;
        this.subflowID = subflowID;
        this.events.addAll(events);
    }


    public SubflowEventsInOrderVerifier hasSubflowStartedEvent() {
        var remainingEvents = List.copyOf(events);
        SubflowEvent nextEvent = getNextEvent();
        assertThat(nextEvent)
                .withFailMessage("Expected SubflowStartedEvent, but was "+nextEvent+". All remaining events: "+remainingEvents)
                .isInstanceOf(SubflowStartedEvent.class)
                .satisfies(event -> assertThat(event.flowID()).isEqualTo(root.id()))
                .satisfies(event -> assertThat(event.subflowID()).isEqualTo(subflowID));
        return this;
    }

    public SubflowEventsInOrderVerifier hasSubflowPausedEvent() {
        var remainingEvents = List.copyOf(events);
        SubflowEvent nextEvent = getNextEvent();
        assertThat(nextEvent)
                .withFailMessage("Expected SubflowPausedEvent, but was "+nextEvent+". All remaining events: "+remainingEvents)
                .isInstanceOf(SubflowPausedEvent.class)
                .satisfies(event -> assertThat(event.flowID()).isEqualTo(root.id()))
                .satisfies(event ->   assertThat(event.subflowID()).isEqualTo(subflowID));

        return this;
    }

    public SubflowEventsInOrderVerifier hasSubflowFailedEvent(RuntimeException exception) {
        var remainingEvents = List.copyOf(events);
        SubflowEvent nextEvent = getNextEvent();
        assertThat(nextEvent)
                .withFailMessage("Expected SubflowFailedEvent, but was "+nextEvent+". All remaining events: "+remainingEvents)
                .asInstanceOf(type(SubflowFailedEvent.class))
                .satisfies(event -> assertThat(event.flowID()).isEqualTo(root.id()))
                .satisfies(event -> assertThat(event.subflowID()).isEqualTo(subflowID))
                .satisfies(event -> assertThat(event.exception()).isEqualTo(exception));
        return this;
    }

    public SubflowEventsInOrderVerifier hasSubflowCompletedEvent() {
        var remainingEvents = List.copyOf(events);
        SubflowEvent nextEvent = getNextEvent();
        assertThat(nextEvent)
                .withFailMessage("Expected SubflowCompletedEvent, but was "+nextEvent+". All remaining events: "+remainingEvents)
                .isInstanceOf(SubflowCompletedEvent.class)
                .satisfies(event -> assertThat(event.flowID()).isEqualTo(root.id()))
                .satisfies(event -> assertThat(event.subflowID()).isEqualTo(subflowID));

        return this;
    }

    public void andHasNoMoreEvents() {
        assertThat(events).isEmpty();
    }

    private SubflowEvent getNextEvent(){
        if(events.isEmpty()) {
            throw new IllegalStateException("No events left");
        }
        return events.removeFirst();
    }
}
