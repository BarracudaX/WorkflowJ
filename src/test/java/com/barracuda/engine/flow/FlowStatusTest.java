package com.barracuda.engine.flow;

import com.barracuda.engine.test.flow.TestFlow;
import org.junit.jupiter.api.Test;

import static com.barracuda.engine.test.assertJ.CustomAssertions.assertThat;
import static com.barracuda.engine.test.builder.TestFlowBuilder.testFlow;

/**
 * Tests related to flow's state.
 */
public class FlowStatusTest {

    @Test
    void newlyCreatedFlowShouldBeInReadyState() {
        TestFlow flow = testFlow()
                .actionTask("Task")
                .build();

        assertThat(flow).isReady();
    }

    @Test
    void runningFlowShouldHaveRunningState() {
        var testFlow = testFlow()
                .actionTask("Task")
                .build()
                .startFlow();

        assertThat(testFlow).isEventuallyRunning();
    }

    @Test
    void shouldHaveCompletedStateOnceFinished() {
        var testFlow = testFlow()
                .actionTask("Task")
                .build()
                .startFlow()
                .finishTask("Task");

        assertThat(testFlow).isEventuallyCompleted();
    }

    @Test
    void shouldHaveFailedStateIfTaskFailsWithException() {
        var exception = new RuntimeException("FAILED");

        var testFlow = testFlow()
                .actionTask("FailTask")
                .build()
                .startFlow()
                .failTask("FailTask", exception);

        assertThat(testFlow).hasEventuallyFailedWith(exception);
    }

    @Test
    void shouldHavePausedStateWhenInterrupted() {
        var testFlow = testFlow()
                .actionTask("FirstTask")
                .build()
                .startFlow()
                .interruptFlow();

        assertThat(testFlow).isEventuallyPaused();
    }
}
