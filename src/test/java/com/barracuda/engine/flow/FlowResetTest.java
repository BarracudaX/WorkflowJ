package com.barracuda.engine.flow;

import com.barracuda.engine.test.flow.TestFlow;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static com.barracuda.engine.test.builder.TestFlowBuilder.testFlow;

@Disabled("Need to figure out what resetting means to each node")
public class FlowResetTest {

    @Test
    void shouldNotAllowResettingRunningFlow() {
        testFlow()
                .task("task")
                .build()
                .startFlow()
                .expectIsRunning()
                .assertThrows(TestFlow::reset, assertError -> assertError.isInstanceOf(IllegalStateException.class));
    }

    @Test
    void shouldAllowResettingCompletedFlow() {
        testFlow()
                .task("task")
                .build()
                .startFlow()
                .assertTaskRunning("task")
                .finishTask("task")
                .expectFlowCompleted()
                .reset()
                .expectFlowReady();
    }

    @Test
    void shouldAllowResettingFailedFlow() {
        testFlow()
                .task("task")
                .build()
                .startFlow()
                .assertTaskRunning("task")
                .failTask("task", new RuntimeException("FAILED"))
                .expectFlowFailed()
                .reset()
                .expectFlowReady();
    }

    @Test
    void shouldAllowResettingPausedFlow() {
        testFlow()
                .task("task")
                .build()
                .startFlow()
                .assertTaskRunning("task")
                .interruptFlowAndExpectFlowPaused()
                .reset()
                .expectFlowReady();
    }
}
