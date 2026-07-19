package com.barracuda.engine.flow;

import com.barracuda.engine.task.Task;
import lombok.Getter;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

public final class TestUtils {

    private TestUtils() {}

    public static void waitUntilFailed(Flow flow) {
        try{
            Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(flow::state, state -> assertThat(state).isEqualTo(FlowState.FAILED));
        }catch (ConditionTimeoutException ex){
            throw new AssertionError("Failed waiting for flow to fail.",ex);
        }
    }

    public static void waitUntilRunning(Flow flow) {
        try{
            Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(flow::state,state -> assertThat(state).isEqualTo(FlowState.RUNNING));
        }catch (ConditionTimeoutException ex){
            throw new AssertionError("Failed waiting for flow to start running.",ex);
        }
    }

    public static void waitUntilPaused(Flow flow) {
        try {
            Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(flow::state,state -> assertThat(state).isEqualTo(FlowState.PAUSED));
        } catch (ConditionTimeoutException ex){
            throw new AssertionError("Failed waiting for flow to pause.",ex);
        }
    }

    public static void waitUntilCompleted(Flow flow) {
        try {
            Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(flow::state, state -> assertThat(state).isEqualTo(FlowState.COMPLETED));
        } catch (ConditionTimeoutException ex){
            throw new AssertionError("Failed waiting for flow to complete.",ex);
        }
    }

    public record ParallelTestTask(CountDownLatch notifyReadyLatch, CountDownLatch barrierLatch) implements Task<Void, Void> {

        @Override
        public Void execute(Void input) {
            notifyReadyLatch.countDown();
            try {
                barrierLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return null;
        }
    }

    /**
     * A task that captures the thread type on which its execute method was called.
     */
    public static class TaskCapturingThread implements Task<Void,Void>{

        public enum TaskThread{
            VIRTUAL,PLATFORM,NONE
        }

        @Getter
        private volatile TaskThread taskThread = TaskThread.NONE;

        @Override
        public Void execute(Void input) {
            if (Thread.currentThread().isVirtual()) {
                taskThread = TaskThread.VIRTUAL;
            }else{
                taskThread = TaskThread.PLATFORM;
            }
            return null;
        }

    }

    /**
     * A task that blocks on a latch that can be released with finish method.
     */
    public static class BlockingTask implements Task<Void, Void> {

        public enum TaskState {
            CREATED, RUNNING,COMPLETED,INTERRUPTED
        }

        private final AtomicReference<TaskState> state = new AtomicReference<>(TaskState.CREATED);
        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public Void execute(Void input) {
            state.set(TaskState.RUNNING);
            try {
                latch.await();
            } catch (InterruptedException e) {
                state.set(TaskState.INTERRUPTED);
                throw new RuntimeException(e);
            }
            state.set(TaskState.COMPLETED);
            return null;
        }

        public void finish(){
            latch.countDown();
        }

        public TaskState state(){
            return state.get();
        }

        public void waitUntilRunning(){
            try {
                Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(this::state,state -> assertThat(state).isEqualTo(TaskState.RUNNING));
            } catch (ConditionTimeoutException ex) {
                throw new AssertionError("Failed waiting for the blocking task to start running.",ex);
            }
        }

        public void waitUntilInterrupted() {
            try {
                Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(this::state,state -> assertThat(state).isEqualTo(TaskState.INTERRUPTED));
            } catch (ConditionTimeoutException ex) {
                throw new AssertionError("Failed waiting for the blocking task to get interrupted.",ex);
            }
        }
    }

    /**
     * This task runs busy-waiting until asked to fail. Throws the provided exceptions when asked to fail.
     */
    public static class FailOnDemandTask implements Task<Void,Void>{

        private final AtomicReference<Boolean> shouldFail = new AtomicReference<>(false);
        private final RuntimeException exception;

        public FailOnDemandTask(RuntimeException exception) {
            this.exception = exception;
        }

        @Override
        public Void execute(Void input) {
            while(!shouldFail.get()){
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            throw exception;
        }

        public void failNow(){
            shouldFail.set(true);
        }
    }
}
