package com.barracuda.engine.flow;

import org.junit.jupiter.api.Test;

import static com.barracuda.engine.test.builder.TestFlowBuilder.testFlow;

/**
 * Tests related to task events
 */
public class TaskEventTests {

    @Test
    void shouldPublishTaskStartedEventWhenExecutingTheTask() {
        testFlow()
                .ioTask("test")
                .build()
                .startFlow()
                .assertTaskEventsInOrder("test",events -> events.hasTaskStartedEvent().andHasNoMoreEvents());
    }

    @Test
    void shouldPublishTaskCompletedEventWhenTaskFinishesNormally() {
        testFlow()
                .ioTask("test")
                .build()
                .startFlow()
                .finishTask("test")
                .assertTaskEventsInOrder("test",events -> events.hasTaskStartedEvent().hasTaskCompletedEvent().andHasNoMoreEvents());
    }

    @Test
    void shouldPublishTaskFailedEventWhenTaskFinishesWithAnException() {
        var exception = new RuntimeException("FAILED");

        testFlow()
                .ioTask("test")
                .build()
                .startFlow()
                .failTask("test", exception)
                .expectFlowFailed()
                .assertTaskEventsInOrder("test",events -> events.hasTaskStartedEvent().hasTaskFailedEvent(exception).andHasNoMoreEvents());
    }

    @Test
    void shouldPublishTaskPausedEventWhenTaskInterrupted() {
        testFlow()
                .ioTask("test")
                .build()
                .startFlow()
                .interruptFlowAndExpectFlowPaused()
                .assertTaskEventsInOrder("test", events -> events.hasTaskStartedEvent().hasTaskPausedEvent().andHasNoMoreEvents());
    }


}
