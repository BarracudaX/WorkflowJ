package com.barracuda.engine.flow;

import com.barracuda.engine.test.assertJ.TestFlowAssert.TestTaskAssert;
import com.barracuda.engine.test.flow.TestFlow;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static com.barracuda.engine.test.assertJ.CustomAssertions.assertThat;
import static com.barracuda.engine.test.builder.TestFlowBuilder.testFlow;

/**
 * Tests related to cancellation/interruption.
 */
public class FlowCancellationTest {

    @Test
    void shouldCancelRunningTaskWhenPaused() {
        TestFlow testFlow = testFlow()
                .ioTask("FirstTask")
                .build()
                .startFlow()
                .interruptFlow()
                .waitUntilPaused();

        assertThat(testFlow).hasTaskSatisfying("FirstTask", TestTaskAssert::isEventuallyCancelled);
    }

    @Disabled("Need to be replaced with a better test that tests that the second task is not started until first is completed. If such test already exists, this need to be deleted.")
    @Test
    void shouldNotExecuteNextTaskWhenInterruptedAndFirstTaskHasNotCompleted() {
    }
}
