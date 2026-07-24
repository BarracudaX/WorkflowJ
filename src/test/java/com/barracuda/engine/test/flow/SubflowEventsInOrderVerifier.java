package com.barracuda.engine.test.flow;

import com.barracuda.engine.event.ExecutionEvent.SubflowEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.SubflowCompletedEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.SubflowFailedEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.SubflowPausedEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.SubflowStartedEvent;
import com.barracuda.engine.flow.Flow;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

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

    public SubflowEventsInOrderVerifier hasSubflowStartedEvent(Consumer<SubflowStartedEvent> consumer){
        consumer.accept(getNextEvent());
        return this;
    }

    public SubflowEventsInOrderVerifier hasSubflowStartedEvent() {
        return hasSubflowStartedEvent(_ -> {});
    }

    public SubflowEventsInOrderVerifier hasSubflowPausedEvent(Consumer<SubflowPausedEvent> consumer) {
        consumer.accept(getNextEvent());
        return this;
    }

    public SubflowEventsInOrderVerifier hasSubflowPausedEvent() {
        return hasSubflowPausedEvent(_ -> {});
    }

    public SubflowEventsInOrderVerifier hasSubflowFailedEvent(Consumer<SubflowFailedEvent> consumer) {
        consumer.accept(getNextEvent());
        return this;
    }

    public SubflowEventsInOrderVerifier hasSubflowFailedEvent() {
        return hasSubflowFailedEvent(_ -> {});
    }

    public SubflowEventsInOrderVerifier hasSubflowCompletedEvent(Consumer<SubflowCompletedEvent> consumer) {
        consumer.accept(getNextEvent());

        return this;
    }

    public SubflowEventsInOrderVerifier hasSubflowCompletedEvent() {
        return hasSubflowCompletedEvent(_ -> {});
    }

    public void andHasNoMoreEvents() {
        assertThat(events).isEmpty();
    }

    private <T extends SubflowEvent> T getNextEvent(){
        if(events.isEmpty()) {
            throw new IllegalStateException("No events left");
        }
        return (T) events.removeFirst();
    }
}
