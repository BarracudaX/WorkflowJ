package com.barracuda.engine.flow;

import org.junit.jupiter.api.Test;

import static com.barracuda.engine.test.TestFlowBuilder.testFlow;

/**
 * Tests related to flow events.
 */
public class FlowEventsTest {

    @Test
    void shouldPublishFlowStartedEventWhenStartingTheFlow() {
        testFlow()
                .task("test")
                .build()
                .startFlow()
                .assertFlowEventsInOrder(events -> events.hasFlowStartedEvent().andHasNoMoreEvents());
    }

    @Test
    void shouldPublishFlowCompletedEventWhenFlowFinishesNormally() {
        testFlow()
                .task("test")
                .build()
                .startFlow()
                .finishTask("test")
                .expectFlowCompleted()
                .assertFlowEventsInOrder(events -> events.hasFlowStartedEvent().hasFlowCompletedEvent().andHasNoMoreEvents());
    }

    @Test
    void shouldPublishFlowFailedEventWhenATaskFailsWithAnException() {
        var exception = new RuntimeException("FAILED");

        testFlow()
                .task("test")
                .build()
                .startFlow()
                .failTask("test", exception)
                .expectFlowFailed()
                .assertFlowEventsInOrder( events -> events.hasFlowStartedEvent().hasFlowFailedEvent(exception).andHasNoMoreEvents());
    }

    @Test
    void shouldPublishFlowPausedEventWhenInterrupted() {
        testFlow()
                .task("test")
                .build()
                .startFlow()
                .interruptFlowAndExpectFlowPaused()
                .assertFlowEventsInOrder(events -> events.hasFlowStartedEvent().hasFlowPausedEvent().andHasNoMoreEvents());
    }


}
