package com.barracuda.engine.flow;

import com.barracuda.engine.test.flow.TestSubflow;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.barracuda.engine.test.builder.TestFlowBuilder.testFlow;

/**
 * Tests related to flow events.
 */
public class FlowEventsTest {

    @Test
    void shouldPublishFlowStartedEventWhenStartingTheFlow() {
        testFlow()
                .ioTask("test")
                .build()
                .startFlow()
                .assertFlowEventsInOrder(events -> events.hasFlowStartedEvent().andHasNoMoreEvents());
    }

    @Test
    void shouldPublishFlowCompletedEventWhenFlowFinishesNormally() {
        testFlow()
                .ioTask("test")
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
                .ioTask("test")
                .build()
                .startFlow()
                .failTask("test", exception)
                .expectFlowFailed()
                .assertFlowEventsInOrder( events -> events.hasFlowStartedEvent().hasFlowFailedEvent(exception).andHasNoMoreEvents());
    }

    @Test
    void shouldPublishFlowPausedEventWhenInterrupted() {
        //need better output for debugging/testing.
        testFlow()
                .ioTask("task1")
                .subflows(new TestSubflow( "Subflow1", List.of("parallelTask1")))
                .subflows(new TestSubflow( "Subflow2", List.of("parallelTask2")))
                .subflows(new TestSubflow( "Subflow3", List.of("parallelTask3")))
                .ioTask("task2")
                .build()
                .startFlow()
                .finishTask("task1")
                .finishTask("parallelTask1")
                .interruptFlowAndExpectFlowPaused()
                .assertFlowEventsInOrder(events -> events.hasFlowStartedEvent().hasFlowPausedEvent().andHasNoMoreEvents());

    }

    @Test
    void shouldPublishSubflowEventsForSubflows() {
        testFlow()
                .ioTask("task1")
                .subflows(new TestSubflow( "Subflow1", List.of("parallelTask1")),new TestSubflow( "Subflow2", List.of("parallelTask2")),new TestSubflow( "Subflow3", List.of("parallelTask3")))
                .ioTask("task2")
                .build()
                .startFlow()
                .finishTask("task1")
                .assertTaskRunning("parallelTask1")
                .finishTask("parallelTask1")
                .interruptFlowAndExpectFlowPaused()
                .assertSubflowEventsInOrder("Subflow1", events -> events.hasSubflowStartedEvent().hasSubflowCompletedEvent().andHasNoMoreEvents())
                .assertSubflowEventsInOrder("Subflow2",events -> events.hasSubflowStartedEvent().hasSubflowPausedEvent().andHasNoMoreEvents())
                .assertSubflowEventsInOrder("Subflow3",events -> events.hasSubflowStartedEvent().hasSubflowPausedEvent().andHasNoMoreEvents());
    }


}
