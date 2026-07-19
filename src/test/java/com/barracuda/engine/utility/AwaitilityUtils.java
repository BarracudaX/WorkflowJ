package com.barracuda.engine.utility;

import com.barracuda.engine.flow.Flow;
import com.barracuda.engine.flow.FlowState;
import com.barracuda.engine.test.TestTask;
import com.barracuda.engine.test.TestTaskState;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

public final class AwaitilityUtils {

    private AwaitilityUtils() {}


    public static void waitUntilFlowFailed(Flow flow) {
        try{
            Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(flow::state, state -> assertThat(state).isEqualTo(FlowState.FAILED));
        }catch (ConditionTimeoutException ex){
            throw new AssertionError("Failed waiting for flow to fail.Current state of the flow is "+flow.state(),ex);
        }
    }

    public static void waitUntilFlowRunning(Flow flow) {
        try{
            Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(flow::state,state -> assertThat(state).isEqualTo(FlowState.RUNNING));
        }catch (ConditionTimeoutException ex){
            throw new AssertionError("Failed waiting for flow to start running.Current state of the flow is "+flow.state(),ex);
        }
    }

    public static void waitUntilFlowPaused(Flow flow) {
        try {
            Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(flow::state,state -> assertThat(state).isEqualTo(FlowState.PAUSED));
        } catch (ConditionTimeoutException ex){
            throw new AssertionError("Failed waiting for flow to pause.Current state of the flow is "+flow.state(),ex);
        }
    }

    public static void waitUntilFlowCompleted(Flow flow) {
        try {
            Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(flow::state, state -> assertThat(state).isEqualTo(FlowState.COMPLETED));
        } catch (ConditionTimeoutException ex){
            throw new AssertionError("Failed waiting for flow to complete.Current state of the flow is "+flow.state(),ex);
        }
    }

    public static void waitUntilTestTaskIsRunning(TestTask task) {
        try {
            Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(task::state,state -> assertThat(state).isEqualTo(TestTaskState.RUNNING));
        } catch (ConditionTimeoutException ex) {
            throw new AssertionError("Failed waiting for the blocking task to start running.Current task sate is "+task.state(),ex);
        }
    }

    public static void waitUntilTestTaskCompleted(TestTask task){
        try {
            Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(task::state,state -> assertThat(state).isEqualTo(TestTaskState.COMPLETED));
        } catch (ConditionTimeoutException ex) {
            throw new AssertionError("Failed waiting for the blocking task to finish.Current task sate is "+task.state(),ex);
        }
    }

    public static void waitUntilTestTaskFailed(TestTask task){
        try {
            Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(task::state,state -> assertThat(state).isEqualTo(TestTaskState.FAILED));
        } catch (ConditionTimeoutException ex) {
            throw new AssertionError("Failed waiting for the blocking task to finish.Current task sate is "+task.state(),ex);
        }
    }

    public static void waitUntilTestTaskInterrupted(TestTask task) {
        try {
            Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(task::state,state -> assertThat(state).isEqualTo(TestTaskState.INTERRUPTED));
        } catch (ConditionTimeoutException ex) {
            throw new AssertionError("Failed waiting for the blocking task to get interrupted.Current task sate is "+task.state(),ex);
        }
    }
}
