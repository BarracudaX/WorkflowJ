package com.barracuda.engine.utility;

import com.barracuda.engine.flow.Flow;
import com.barracuda.engine.flow.FlowStatus;
import com.barracuda.engine.test.task.TestTask;
import com.barracuda.engine.test.task.TestTaskState;
import org.awaitility.Awaitility;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

public final class AwaitilityUtils {

    private AwaitilityUtils() {}

    public static void waitUntilFlowFailed(Flow flow, Duration duration) {
        Awaitility.await().atMost(duration).untilAsserted(flow::status, state -> assertThat(state).isEqualTo(FlowStatus.FAILED));
    }

    public static void waitUntilFlowRunning(Flow flow,Duration duration) {
        Awaitility.await().atMost(duration).untilAsserted(flow::status, state -> assertThat(state).isEqualTo(FlowStatus.RUNNING));
    }

    public static void waitUntilFlowPaused(Flow flow,Duration duration) {
        Awaitility.await().atMost(duration).untilAsserted(flow::status, state -> assertThat(state).isEqualTo(FlowStatus.PAUSED));
     }

    public static void waitUntilFlowCompleted(Flow flow, Duration duration) {
        Awaitility.await().atMost(duration).untilAsserted(flow::status, state -> assertThat(state).isEqualTo(FlowStatus.COMPLETED));
    }

    public static void waitUntilTestTaskIsRunning(TestTask task, Duration duration) {
        Awaitility.await().atMost(duration).untilAsserted(task::state,state -> assertThat(state).isEqualTo(TestTaskState.RUNNING));
     }

    public static void waitUntilTestTaskCompleted(TestTask task, Duration duration){
        Awaitility.await().atMost(duration).untilAsserted(task::state,state -> assertThat(state).isEqualTo(TestTaskState.COMPLETED));
    }

    public static void waitUntilTestTaskFailed(TestTask task, Duration duration){
        Awaitility.await().atMost(duration).untilAsserted(task::state,state -> assertThat(state).isEqualTo(TestTaskState.FAILED));
    }

    public static void waitUntilTestTaskInterrupted(TestTask task, Duration duration) {
        Awaitility.await().atMost(duration).untilAsserted(task::state,state -> assertThat(state).isEqualTo(TestTaskState.INTERRUPTED));
    }
}
