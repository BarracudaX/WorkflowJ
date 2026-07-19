package com.barracuda.engine.test;

import com.barracuda.engine.task.Task;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static com.barracuda.engine.utility.AwaitilityUtils.*;

/**
 * A task that blocks on a latch that can be asked to either finish normally or with an exception.
 * Note that before calling finish or fail, use waiUntilRunning to verify that the task runs; otherwise, an IllegalStateException will be thrown because the task isn't running.
 */
public class TestTask implements Task<Void, Void> {

    public enum TaskThread {
        VIRTUAL, PLATFORM, NONE
    }

    private final AtomicReference<TestTaskState> state = new AtomicReference<>(TestTaskState.CREATED);
    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile RuntimeException failException;
    private final long id;
    private final AtomicReference<TaskThread> taskThread = new AtomicReference<>(TaskThread.NONE);

    public TestTask(long id) {
        this.id = id;
    }

    @Override
    public Void execute(Void input) {
        if (Thread.currentThread().isVirtual()) {
            taskThread.set(TaskThread.VIRTUAL);
        } else {
            taskThread.set(TaskThread.PLATFORM);
        }

        state.set(TestTaskState.RUNNING);

        try {
            latch.await();
            if (failException != null) {
                state.set(TestTaskState.FAILED);
                throw failException;
            }
        } catch (InterruptedException ex) {
            state.set(TestTaskState.INTERRUPTED);
            throw new RuntimeException(ex);
        }

        return null;
    }

    @Override
    public long id() {
        return id;
    }

    public TestTask failNow(RuntimeException failException) {
        if (!state.compareAndSet(TestTaskState.RUNNING, TestTaskState.COMPLETED)) {
            throw new IllegalStateException("Cannot make this task fail because its state is not RUNNING, but " + state.get());
        }
        this.failException = Objects.requireNonNull(failException);
        latch.countDown();

        return this;
    }

    public TestTask finish() {
        if (!state.compareAndSet(TestTaskState.RUNNING, TestTaskState.COMPLETED)) {
            throw new IllegalStateException("Cannot finish this task because its state is not RUNNING, but " + state.get());
        }
        latch.countDown();

        return this;
    }

    public void waitUntilRunning(){ waitUntilTestTaskIsRunning(this);}

    public void waitUntilCompleted() {
        waitUntilTestTaskCompleted(this);
    }

    public void waitUntilFailed() { waitUntilTestTaskFailed(this);}

    public void waitUntilPaused() { waitUntilTestTaskInterrupted(this); }

    public TestTaskState state() {
        return state.get();
    }

    public TaskThread taskThread() {
        return taskThread.get();
    }
}
