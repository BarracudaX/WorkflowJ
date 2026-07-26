package com.barracuda.engine.flow;

import com.barracuda.engine.test.flow.TestFlow;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static com.barracuda.engine.test.builder.TestFlowBuilder.testFlow;

@Disabled("Need to figure out what resetting means to each node")
public class FlowResetTest {

//    @Test
//    void shouldNotAllowResettingRunningFlow() {
//        TestFlow testFlow = testFlow()
//                .ioTask("task")
//                .build()
//                .startFlow();
//    }
//
//    @Test
//    void shouldAllowResettingCompletedFlow() {
//        testFlow()
//                .ioTask("task")
//                .build()
//                .startFlow()
//                .assertTaskRunning("task")
//                .finishTask("task")
//                .waitUntilCompleted()
//                .reset()
//                .waitUntilReady();
//    }
//
//    @Test
//    void shouldAllowResettingFailedFlow() {
//        testFlow()
//                .ioTask("task")
//                .build()
//                .startFlow()
//                .assertTaskRunning("task")
//                .failTask("task", new RuntimeException("FAILED"))
//                .waitUntilFailed()
//                .reset()
//                .waitUntilReady();
//    }
//
//    @Test
//    void shouldAllowResettingPausedFlow() {
//        testFlow()
//                .ioTask("task")
//                .build()
//                .startFlow()
//                .assertTaskRunning("task")
//                .interruptFlow()
//                .reset()
//                .waitUntilReady();
//    }
}
