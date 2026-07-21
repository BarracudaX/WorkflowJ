package com.barracuda.engine.flow;

import org.junit.jupiter.api.Test;

import static com.barracuda.engine.test.TestFlowBuilder.testFlow;

/**
 * Tests related to task evenets
 */
public class TaskEventTests {

    @Test
    void shouldPublishTaskStartedEventWhenExecutingTheTask() {
        testFlow()
                .task("test")
                .build()
                .startFlow()
                .assertTaskEventsInOrder("test",events -> events.hasTaskStartedEvent().andHasNoMoreEvents());
    }

    @Test
    void shouldPublishTaskCompletedEventWhenTaskFinishesNormally() {
        testFlow()
                .task("test")
                .build()
                .startFlow()
                .finishTask("test")
                .assertTaskEventsInOrder("test",events -> events.hasTaskStartedEvent().hasTaskCompletedEvent().andHasNoMoreEvents());
    }

    @Test
    void shouldPublishTaskFailedEventWhenTaskFinishesWithAnException() {
        var exception = new RuntimeException("FAILED");

        testFlow()
                .task("test")
                .build()
                .startFlow()
                .failTask("test", exception)
                .expectFlowFailed()
                .assertTaskEventsInOrder("test",events -> events.hasTaskStartedEvent().hasTaskFailedEvent(exception).andHasNoMoreEvents());
    }

    @Test
    void shouldPublishTaskPausedEventWhenTaskInterrupted() {
        testFlow()
                .task("test")
                .build()
                .startFlow()
                .interruptFlowAndExpectFlowPaused()
                .assertTaskEventsInOrder("test", events -> events.hasTaskStartedEvent().hasTaskPausedEvent().andHasNoMoreEvents());
    }


}
