package com.barracuda.engine.flow;

import com.barracuda.engine.event.ExecutionEvent;
import com.barracuda.engine.event.ExecutionEvent.TaskEvent.TaskCompletedEvent;
import com.barracuda.engine.event.ExecutionEvent.TaskEvent.TaskFailedEvent;
import com.barracuda.engine.event.ExecutionEvent.TaskEvent.TaskPausedEvent;
import com.barracuda.engine.event.ExecutionEvent.TaskEvent.TaskStartEvent;
import com.barracuda.engine.test.flow.TestFlow;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static com.barracuda.engine.test.assertJ.CustomAssertions.assertThat;
import static com.barracuda.engine.test.builder.TestFlowBuilder.testFlow;

/**
 * Tests related to task events
 */
public class TaskEventTests {

    @Test
    void shouldPublishTaskStartedEventWhenExecutingTheTask() {
        TestFlow testFlow = testFlow()
                .ioTask("test")
                .build()
                .startFlow();

        assertThat(testFlow).taskEventsSatisfying("test", events -> events.nextEventIs(TaskStartEvent.class).andHasNoMoreEvents());
    }

    @Test
    void shouldPublishTaskCompletedEventWhenTaskFinishesNormally() {
        TestFlow testFlow = testFlow()
                .ioTask("test")
                .build()
                .startFlow()
                .finishTask("test");

        assertThat(testFlow).taskEventsSatisfying("test",events -> events.nextEventIs(TaskStartEvent.class).nextEventIs(TaskCompletedEvent.class).andHasNoMoreEvents());
    }

    @Test
    void shouldPublishTaskFailedEventWhenTaskFinishesWithAnException() {
        var exception = new RuntimeException("FAILED");

        TestFlow testFlow = testFlow()
                .ioTask("test")
                .build()
                .startFlow()
                .failTask("test", exception);

        assertThat(testFlow).taskEventsSatisfying("test", events -> events
                .nextEventIs(TaskStartEvent.class)
                .nextEventIs(TaskFailedEvent.class, event -> assertThat(event.exception()).isEqualTo(exception))
                .andHasNoMoreEvents()
        );
    }

    @Test
    void shouldPublishTaskPausedEventWhenTaskInterrupted() {
        TestFlow testFlow = testFlow()
                .ioTask("test")
                .build()
                .startFlow()
                .interruptFlow();

        assertThat(testFlow).taskEventsSatisfying("test", events -> events.nextEventIs(TaskStartEvent.class).nextEventIs(TaskPausedEvent.class).andHasNoMoreEvents());
    }


}
