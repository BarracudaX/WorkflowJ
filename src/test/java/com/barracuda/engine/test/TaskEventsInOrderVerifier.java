package com.barracuda.engine.test;

import com.barracuda.engine.event.ExecutionEvent.TaskEvent;
import com.barracuda.engine.event.ExecutionEvent.TaskEvent.TaskCompletedEvent;
import com.barracuda.engine.event.ExecutionEvent.TaskEvent.TaskPausedEvent;

import java.util.ArrayList;
import java.util.List;

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

    public TaskEventsInOrderVerifier hasTaskStartedEvent() {
        assertThat(getNextEvent())
                .isInstanceOf(TaskEvent.TaskStartEvent.class)
                .satisfies(event -> assertThat(event.taskID()).isEqualTo(task.id()));
        return this;
    }

    public TaskEventsInOrderVerifier hasTaskPausedEvent() {
        assertThat(getNextEvent())
                .asInstanceOf(type(TaskPausedEvent.class))
                .satisfies(event -> assertThat(event.taskID()).isEqualTo(task.id()));
        return this;
    }

    public TaskEventsInOrderVerifier hasTaskFailedEvent(Exception exception) {
        assertThat(getNextEvent())
                .asInstanceOf(type(TaskEvent.TaskFailedEvent.class))
                .satisfies(event -> assertThat(event.taskID()).isEqualTo(task.id()))
                .satisfies( event -> assertThat(event.exception()).isEqualTo(exception));

        return this;
    }

    public TaskEventsInOrderVerifier hasTaskCompletedEvent() {
        assertThat(getNextEvent())
                .isInstanceOf(TaskCompletedEvent.class)
                .satisfies(event -> assertThat(event.taskID()).isEqualTo(task.id()));

        return this;
    }

    private TaskEvent getNextEvent() {
        if (events.isEmpty()) {
            throw new IllegalStateException("No events left");
        }
        return events.removeFirst();
    }
}
