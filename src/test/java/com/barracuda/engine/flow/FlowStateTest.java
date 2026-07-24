package com.barracuda.engine.flow;

import com.barracuda.engine.test.flow.TestFlow;
import org.assertj.core.api.Assertions;
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

    @Test
    void shouldBeAllowedToTransitionToReplayModeWhenInReadyState() {
        testFlow()
                .ioTask("task")
                .build()
                .expectFlowReady()
                .replayMode()
                .expectFlowInReplayMode();
    }

    @Test
    void shouldNotBeAbleToTransitionToReplayModeWhenRunning() {
        testFlow()
                .ioTask("task")
                .build()
                .startFlow()
                .assertThrows(TestFlow::replayMode, error -> error.isInstanceOf(IllegalStateException.class));
    }

    @Test
    void shouldNotBeAbleToTransitionToReplayModeWhenFlowInFailedState() {
        testFlow()
                .ioTask("task")
                .build()
                .startFlow()
                .failTask("task", new RuntimeException("FAILED"))
                .expectFlowFailed()
                .assertThrows(TestFlow::replayMode, error -> error.isInstanceOf(IllegalStateException.class));
    }

    @Test
    void shouldNotBeAbleToTransitionToReplayModeWhenFlowInCompletedState() {
        testFlow()
                .ioTask("task")
                .build()
                .startFlow()
                .finishTask("task")
                .expectFlowCompleted()
                .assertThrows(TestFlow::replayMode, error -> error.isInstanceOf(IllegalStateException.class));
    }

    @Test
    void shouldNotBeAbleToTransitionToReplayModeWhenFlowInPausedState() {
        testFlow()
                .ioTask("task")
                .build()
                .startFlow()
                .interruptFlowAndExpectFlowPaused()
                .assertThrows(TestFlow::replayMode, error -> error.isInstanceOf(IllegalStateException.class));
    }

    @Test
    void shouldAllowSendingEnterReplayModeCommandWhenAlreadyInReplayMode() {
        Assertions.assertThatCode(() -> testFlow()
                .ioTask("task")
                .build()
                .replayMode()
                .replayMode()).doesNotThrowAnyException();
    }
}
