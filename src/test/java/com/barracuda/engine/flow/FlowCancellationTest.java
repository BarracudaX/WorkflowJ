package com.barracuda.engine.flow;

import org.junit.jupiter.api.Test;

import static com.barracuda.engine.test.TestFlowBuilder.testFlow;

/**
 * Tests related to cancellation/interruption.
 */
public class FlowCancellationTest {

    @Test
    void shouldCancelAllRunningTaskWhenPaused() {
        testFlow()
                .task("FirstTask")
                .build()
                .startFlow()
                .interruptFlowAndExpectFlowPaused()
                .assertTaskCancelled("FirstTask");
    }

    @Test
    void shouldNotExecuteNextTaskWhenInterrupted() {
        testFlow()
                .task("FirstTask")
                .task("SecondTask")
                .build()
                .startFlow()
                .assertTaskRunning("FirstTask")
                .interruptFlowAndExpectFlowPaused()
                .assertTaskCancelled("FirstTask")
                .assertTaskNotStarted("SecondTask");
    }
}
