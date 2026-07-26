package com.barracuda.engine.flow;

import com.barracuda.engine.test.flow.TestFlow;
import org.junit.jupiter.api.Test;

import static com.barracuda.engine.test.assertJ.CustomAssertions.assertThat;
import static com.barracuda.engine.test.builder.TestFlowBuilder.testFlow;

public class FlowReplayModeTest {

    @Test
    void shouldBeAbleToTransitionFromReplayModeToReady() {
        TestFlow testFlow = testFlow()
                .ioTask("task")
                .build()
                .enterReplayMode();
        assertThat(testFlow).enteredReplayMode();

        testFlow.prepare();

        assertThat(testFlow).isReady();
    }

}
