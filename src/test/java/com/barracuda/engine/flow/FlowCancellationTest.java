package com.barracuda.engine.flow;

import org.junit.jupiter.api.Test;

import static com.barracuda.engine.test.builder.TestFlowBuilder.testFlow;

/**
 * Tests related to cancellation/interruption.
 */
public class FlowCancellationTest {

    @Test
    void shouldCancelAllRunningTaskWhenPaused() {
        testFlow()
                .ioTask("FirstTask")
                .build()
                .startFlow()
                .interruptFlowAndExpectFlowPaused()
                .assertTaskCancelled("FirstTask");
    }

    @Test
    void shouldNotExecuteNextTaskWhenInterrupted() {
        testFlow()
                .ioTask("FirstTask")
                .ioTask("SecondTask")
                .build()
                .startFlow()
                .assertTaskRunning("FirstTask")
                .interruptFlowAndExpectFlowPaused()
                .assertTaskCancelled("FirstTask")
                .assertTaskNotStarted("SecondTask");
    }
}
