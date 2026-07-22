package com.barracuda.engine.test.task;

import com.barracuda.engine.task.Task;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static com.barracuda.engine.utility.AwaitilityUtils.*;

/**
 * A task that blocks on a latch that can be asked to either finish normally or with an exception.
 * Note that before calling finish or fail, use waiUntilRunning to verify that the task runs; otherwise, an IllegalStateException will be thrown because the task isn't running.
 */
public class TestTask<I> implements Task<I, Void> {

    public enum TaskThread {
        VIRTUAL, PLATFORM, NONE
    }

    private final AtomicReference<TestTaskState> state = new AtomicReference<>(TestTaskState.CREATED);
    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile RuntimeException failException;
    private volatile I input;
    private final long id;
    private final AtomicReference<TaskThread> taskThread = new AtomicReference<>(TaskThread.NONE);

    public TestTask(long id) {
        this.id = id;
    }

    @Override
    public Void execute(I input) {
        if (Thread.currentThread().isVirtual()) {
            taskThread.set(TaskThread.VIRTUAL);
        } else {
            taskThread.set(TaskThread.PLATFORM);
        }

        state.set(TestTaskState.RUNNING);

        try {
            this.input = input;
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

    public TestTask<I> failNow(RuntimeException failException) {
        if (!state.compareAndSet(TestTaskState.RUNNING, TestTaskState.COMPLETED)) {
            throw new IllegalStateException("Cannot make this task fail because its state is not RUNNING, but " + state.get());
        }
        this.failException = Objects.requireNonNull(failException);
        latch.countDown();

        return this;
    }

    public TestTask<I> finish() {
        if (!state.compareAndSet(TestTaskState.RUNNING, TestTaskState.COMPLETED)) {
            throw new IllegalStateException("Cannot finish this task because its state is not RUNNING, but " + state.get());
        }
        latch.countDown();

        return this;
    }

    public void waitUntilRunning(Duration duration){ waitUntilTestTaskIsRunning(this, duration);}

    public void waitUntilCompleted(Duration duration) {
        waitUntilTestTaskCompleted(this, duration);
    }

    public void waitUntilFailed(Duration duration) { waitUntilTestTaskFailed(this, duration);}

    public void waitUntilPaused(Duration duration) { waitUntilTestTaskInterrupted(this, duration); }

    public TestTaskState state() {
        return state.get();
    }

    public TaskThread taskThread() {
        return taskThread.get();
    }

    public I input() {
        return input;
    }
}
