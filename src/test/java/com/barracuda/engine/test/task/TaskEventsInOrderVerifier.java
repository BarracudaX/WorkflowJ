package com.barracuda.engine.test.task;

import com.barracuda.engine.event.ExecutionEvent.TaskEvent;
import com.barracuda.engine.event.ExecutionEvent.TaskEvent.TaskCompletedEvent;
import com.barracuda.engine.event.ExecutionEvent.TaskEvent.TaskFailedEvent;
import com.barracuda.engine.event.ExecutionEvent.TaskEvent.TaskPausedEvent;
import com.barracuda.engine.event.ExecutionEvent.TaskEvent.TaskStartEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

public class TaskEventsInOrderVerifier {

    private final TestTask<?> task;
    private final List<TaskEvent> events = new ArrayList<>();

    public TaskEventsInOrderVerifier(TestTask<?> task, List<TaskEvent> events) {
        this.task = task;
        this.events.addAll(events);
    }

    public void andHasNoMoreEvents(){
        assertThat(events).isEmpty();
    }

    public TaskEventsInOrderVerifier hasTaskStartedEvent(Consumer<TaskStartEvent> consumer) {
        consumer.accept(getNextEvent());
        return this;
    }

    public TaskEventsInOrderVerifier hasTaskStartedEvent() {
        return hasTaskStartedEvent(_ -> {});
    }

    public TaskEventsInOrderVerifier hasTaskPausedEvent(Consumer<TaskPausedEvent> consumer) {
        consumer.accept(getNextEvent());
        return this;
    }

    public TaskEventsInOrderVerifier hasTaskPausedEvent() {
        return  hasTaskPausedEvent(_ -> {});
    }

    public TaskEventsInOrderVerifier hasTaskFailedEvent(Consumer<TaskFailedEvent> consumer) {
        consumer.accept(getNextEvent());
        return this;
    }

    public TaskEventsInOrderVerifier hasTaskFailedEvent() {
        return hasTaskFailedEvent(_ -> {});
    }

    public TaskEventsInOrderVerifier hasTaskCompletedEvent(Consumer<TaskCompletedEvent> consumer) {
        consumer.accept(getNextEvent());
        return this;
    }

    public TaskEventsInOrderVerifier hasTaskCompletedEvent() {
        return hasTaskCompletedEvent(_ -> {});
    }

    private <T extends TaskEvent> T getNextEvent() {
        if (events.isEmpty()) {
            throw new IllegalStateException("No events left");
        }
        return (T) events.removeFirst();
    }
}
