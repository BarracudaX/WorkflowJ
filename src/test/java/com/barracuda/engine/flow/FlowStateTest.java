package com.barracuda.engine.flow;

import org.junit.jupiter.api.Test;

import static com.barracuda.engine.test.builder.TestFlowBuilder.testFlow;

/**
 * Tests related to flow's state.
 */
public class FlowStateTest {

    @Test
    void newlyCreatedFlowShouldBeInCreatedState() {
        testFlow()
                .task("Task")
                .build()
                .expectFlowInCreatedState();
    }

    @Test
    void runningFlowShouldHaveRunningState() {
        testFlow()
                .task("Task")
                .build()
                .startFlow()
                .expectIsRunning();
    }

    @Test
    void shouldHaveCompletedStateOnceFinished() {
        testFlow()
                .task("Task")
                .build()
                .startFlow()
                .finishTask("Task")
                .expectFlowCompleted();
    }

    @Test
    void shouldHaveFailedStateIfTaskFailsWithException() {
        var exception = new RuntimeException("FAILED");
        testFlow()
                .task("FailTask")
                .build()
                .startFlow()
                .failTask("FailTask", exception)
                .expectFlowFailed(exception);
    }

    @Test
    void shouldHavePausedStateWhenInterrupted() {
        testFlow()
                .task("FirstTask")
                .build()
                .startFlow()
                .interruptFlowAndExpectFlowPaused();
    }

}
