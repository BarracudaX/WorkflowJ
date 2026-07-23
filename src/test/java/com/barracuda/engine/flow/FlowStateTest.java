package com.barracuda.engine.flow;

import org.junit.jupiter.api.Test;

import static com.barracuda.engine.test.builder.TestFlowBuilder.testFlow;

/**
 * Tests related to flow's state.
 */
public class FlowStateTest {

    @Test
    void newlyCreatedFlowShouldBeInReadyState() {
        testFlow()
                .ioTask("Task")
                .build()
                .expectFlowReady();
    }

    @Test
    void runningFlowShouldHaveRunningState() {
        testFlow()
                .ioTask("Task")
                .build()
                .startFlow()
                .expectIsRunning();
    }

    @Test
    void shouldHaveCompletedStateOnceFinished() {
        testFlow()
                .ioTask("Task")
                .build()
                .startFlow()
                .finishTask("Task")
                .expectFlowCompleted();
    }

    @Test
    void shouldHaveFailedStateIfTaskFailsWithException() {
        var exception = new RuntimeException("FAILED");
        testFlow()
                .ioTask("FailTask")
                .build()
                .startFlow()
                .failTask("FailTask", exception)
                .expectFlowFailed(exception);
    }

    @Test
    void shouldHavePausedStateWhenInterrupted() {
        testFlow()
                .ioTask("FirstTask")
                .build()
                .startFlow()
                .interruptFlowAndExpectFlowPaused();
    }

}
