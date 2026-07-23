package com.barracuda.engine.test.task;

import com.barracuda.engine.event.ExecutionEvent.TaskEvent;
import com.barracuda.engine.task.Task;
import com.barracuda.engine.test.task.TestTaskInput.TestTaskDataInput;
import com.barracuda.engine.test.task.TestTaskInput.TestTaskNullInput;

import java.time.Duration;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static com.barracuda.engine.utility.AwaitilityUtils.*;

/**
 * A task that blocks on a latch that can be asked to either finish normally or with an exception.
 * Note that before calling finish or fail, use waiUntilRunning to verify that the task runs; otherwise, an IllegalStateException will be thrown because the task isn't running.
 */
public final class TestTask<I> implements Task<I, Void> {

    public enum TaskThread {
        VIRTUAL, PLATFORM, NONE;
    }
    private final AtomicReference<TestTaskState> state = new AtomicReference<>(TestTaskState.CREATED);

    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile RuntimeException failException;
    private volatile Deque<TestTaskInput<I>> input_history = new ConcurrentLinkedDeque<>();
    private final long id;
    private final String name;
    private Deque<TaskEvent> events = new ConcurrentLinkedDeque<>();
    private final Deque<Thread> thread_history = new ConcurrentLinkedDeque<>();
    public TestTask(long id, String name) {
        this.id = id;
        this.name = name;
    }

    @Override
    public Void execute(I input) {
        thread_history.add(Thread.currentThread());

        state.set(TestTaskState.RUNNING);

        try {
            if (input == null) {
                input_history.add(new TestTaskNullInput<>());
            }else{
                input_history.add(new TestTaskDataInput<>(input));
            }

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

    public void waitUntilCompleted(Duration duration) {
        waitUntilTestTaskCompleted(this, duration);
    }

    public void waitUntilFailed(Duration duration) { waitUntilTestTaskFailed(this, duration);}

    public TestTaskState state() {
        return state.get();
    }

    public TaskThread lastTaskThread() {
        var last_thread = thread_history.peekLast();
        if (last_thread == null) {
            return TaskThread.NONE;
        }

        if (last_thread.isVirtual()) {
            return TaskThread.VIRTUAL;
        } else {
            return TaskThread.PLATFORM;
        }
    }

    public I lastInput() {
        TestTaskInput<I> lastInput = input_history.getLast();

        if (lastInput instanceof TestTaskDataInput(I input)) {
            return input;
        }else{
            return null;
        }
    }

    public void event(TaskEvent event) {
        if (event.taskID() == id) {
            events.add(event);
        }
    }

    @Override
    public String toString() {
        return "TestTask{" +
                "state=" + state +
                ", latch=" + latch +
                ", failException=" + failException +
                ", inputs=" + input_history +
                ", id=" + id +
                ", name='" + name + '\'' +
                ", events=" + events +
                ", thread history=" + thread_history +
                '}';
    }
}
