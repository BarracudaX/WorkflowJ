package com.barracuda.engine.flow;

import com.barracuda.engine.test.flow.TestFlow;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static com.barracuda.engine.test.assertJ.CustomAssertions.assertThat;
import static com.barracuda.engine.test.builder.TestFlowBuilder.testFlow;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests related to flow's state.
 */
public class FlowStateTest {

    @Test
    void newlyCreatedFlowShouldBeInReadyState() {
        testFlow()
                .ioTask("Task")
                .build()
                .waitUntilReady();
    }

    @Test
    void runningFlowShouldHaveRunningState() {
        var testFlow = testFlow()
                .ioTask("Task")
                .build()
                .startFlow();

        assertThat(testFlow).isEventuallyRunning();
    }

    @Test
    void shouldHaveCompletedStateOnceFinished() {
        var testFlow = testFlow()
                .ioTask("Task")
                .build()
                .startFlow()
                .finishTask("Task");

        assertThat(testFlow).isEventuallyCompleted();
    }

    @Test
    void shouldHaveFailedStateIfTaskFailsWithException() {
        var exception = new RuntimeException("FAILED");

        var testFlow = testFlow()
                .ioTask("FailTask")
                .build()
                .startFlow()
                .failTask("FailTask", exception);

        assertThat(testFlow).hasEventuallyFailedWith(exception);
    }

    @Test
    void shouldHavePausedStateWhenInterrupted() {
        var testFlow = testFlow()
                .ioTask("FirstTask")
                .build()
                .startFlow()
                .interruptFlow();

        assertThat(testFlow).isEventuallyPaused();
    }

    @Test
    void shouldBeAllowedToTransitionToReplayModeWhenInReadyState() {
        var testFlow = testFlow()
                .ioTask("task")
                .build()
                .replayMode();

        assertThat(testFlow).enteredEventuallyReplayMode();
    }

    @Test
    void shouldNotBeAbleToTransitionToReplayModeWhenRunning() {
        var testFlow = testFlow()
                .ioTask("task")
                .build()
                .startFlow();

        assertThatThrownBy(testFlow::replayMode).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldNotBeAbleToTransitionToReplayModeWhenFlowInFailedState() {
        var testFlow = testFlow()
                .ioTask("task")
                .build()
                .startFlow()
                .failTask("task", new RuntimeException("FAILED"))
                .waitUntilFailed();

        assertThatThrownBy(testFlow::replayMode).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldNotBeAbleToTransitionToReplayModeWhenFlowInCompletedState() {
        var testFlow = testFlow()
                .ioTask("task")
                .build()
                .startFlow()
                .finishTask("task")
                .waitUntilCompleted();

        assertThatThrownBy(testFlow::replayMode).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldNotBeAbleToTransitionToReplayModeWhenFlowInPausedState() {
        TestFlow testFlow = testFlow()
                .ioTask("task")
                .build()
                .startFlow()
                .interruptFlow()
                .waitUntilPaused();

        assertThatThrownBy(testFlow::replayMode).isInstanceOf(IllegalStateException.class);
    }

    @Disabled("Currently not allowed.")
    @Test
    void shouldAllowSendingEnterReplayModeCommandWhenAlreadyInReplayMode() {
        Assertions.assertThatCode(() -> testFlow()
                .ioTask("task")
                .build()
                .replayMode()
                .replayMode()).doesNotThrowAnyException();
    }
}
