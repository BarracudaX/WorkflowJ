package com.barracuda.engine.flow;

import com.barracuda.engine.task.Task;
import lombok.Getter;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

public final class TestUtils {

    private TestUtils() {}

    public static void waitUntilFailed(Flow flow) {
        try{
            Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(flow::state, state -> assertThat(state).isEqualTo(FlowState.FAILED));
        }catch (ConditionTimeoutException ex){
            throw new AssertionError("Failed waiting for flow to fail.Current state of the flow is "+flow.state(),ex);
        }
    }

    public static void waitUntilRunning(Flow flow) {
        try{
            Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(flow::state,state -> assertThat(state).isEqualTo(FlowState.RUNNING));
        }catch (ConditionTimeoutException ex){
            throw new AssertionError("Failed waiting for flow to start running.Current state of the flow is "+flow.state(),ex);
        }
    }

    public static void waitUntilPaused(Flow flow) {
        try {
            Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(flow::state,state -> assertThat(state).isEqualTo(FlowState.PAUSED));
        } catch (ConditionTimeoutException ex){
            throw new AssertionError("Failed waiting for flow to pause.Current state of the flow is "+flow.state(),ex);
        }
    }

    public static void waitUntilCompleted(Flow flow) {
        try {
            Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(flow::state, state -> assertThat(state).isEqualTo(FlowState.COMPLETED));
        } catch (ConditionTimeoutException ex){
            throw new AssertionError("Failed waiting for flow to complete.Current state of the flow is "+flow.state(),ex);
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
    public enum TestTaskState {
        CREATED, RUNNING,COMPLETED,INTERRUPTED,FAILED
    }

    /**
     * A task that blocks on a latch that can be asked to either finish normally or with an exception.
     * Note that before calling finish or fail, use waiUntilRunning to verify that the task runs; otherwise, an IllegalStateException will be thrown because the task isn't running.
     */
    public static class TestTask implements Task<Void,Void>{

        private final AtomicReference<TestTaskState> state = new AtomicReference<>(TestTaskState.CREATED);
        private final CountDownLatch latch = new CountDownLatch(1);
        private volatile RuntimeException failException;

        @Override
        public Void execute(Void input) {
            state.set(TestTaskState.RUNNING);

            try{
                latch.await();
                if (failException != null) {
                    throw failException;
                }
            }catch (InterruptedException ex){
                state.set(TestTaskState.INTERRUPTED);
                throw new RuntimeException(ex);
            }

            return null;
        }

        public void failNow(RuntimeException failException){
            if( !state.compareAndSet(TestTaskState.RUNNING, TestTaskState.COMPLETED)){
                throw new IllegalStateException("Cannot make this task fail because its state is not RUNNING, but "+state.get());
            }
            this.failException = Objects.requireNonNull(failException);
            latch.countDown();
        }

        public void finish(){
            if( !state.compareAndSet(TestTaskState.RUNNING, TestTaskState.COMPLETED)){
                throw new IllegalStateException("Cannot finish this task because its state is not RUNNING, but "+state.get());
            }
            latch.countDown();
        }

        public void waitUntilRunning(){
            try {
                Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(this::state,state -> assertThat(state).isEqualTo(TestTaskState.RUNNING));
            } catch (ConditionTimeoutException ex) {
                throw new AssertionError("Failed waiting for the blocking task to start running.Current task sate is "+state,ex);
            }
        }

        public void waitUntilFinished(){
            try {
                Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(this::state,state -> assertThat(state).isEqualTo(TestTaskState.COMPLETED));
            } catch (ConditionTimeoutException ex) {
                throw new AssertionError("Failed waiting for the blocking task to finish.Current task sate is "+state,ex);
            }
        }

        public void waitUntilFailed(){
            try {
                Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(this::state,state -> assertThat(state).isEqualTo(TestTaskState.FAILED));
            } catch (ConditionTimeoutException ex) {
                throw new AssertionError("Failed waiting for the blocking task to finish.Current task sate is "+state,ex);
            }
        }

        public void waitUntilInterrupted() {
            try {
                Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(this::state,state -> assertThat(state).isEqualTo(TestTaskState.INTERRUPTED));
            } catch (ConditionTimeoutException ex) {
                throw new AssertionError("Failed waiting for the blocking task to get interrupted.Current task sate is "+state,ex);
            }
        }

        public TestTaskState state(){
            return state.get();
        }
    }

}
