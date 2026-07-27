package com.barracuda.engine.test.task;

import com.barracuda.engine.event.ExecutionEvent.TaskEvent;
import com.barracuda.engine.task.ActionTask;

import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A task that blocks on a latch that can be asked to either finish normally or with an exception.
 * Note that before calling finish or fail, use waiUntilRunning to verify that the task runs; otherwise, an IllegalStateException will be thrown because the task isn't running.
 * This class is meant for testing purposes only. Note that this class isn't how tasks typically should be implemented; specifically, tasks shouldn't carry any state that is relevant to the execution of their logic.
 */
public final class TestTask implements ActionTask {

    public enum TaskThread {
        VIRTUAL, PLATFORM, NONE
    }
    private final AtomicReference<TestTaskState> state = new AtomicReference<>(TestTaskState.READY);

    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile RuntimeException failException;
    private final long id;
    private final String name;
    private final Deque<TaskEvent> events = new ConcurrentLinkedDeque<>();
    private final Deque<Thread> thread_history = new ConcurrentLinkedDeque<>();

    public TestTask(long id, String name) {
        this.id = id;
        this.name = name;
    }

    @Override
    public void execute() {
        thread_history.add(Thread.currentThread());

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

    public void event(TaskEvent event) {
        if (event.taskID() == id) {
            events.add(event);
        }
    }

    public String name(){
        return name;
    }

    @Override
    public String toString() {
        return "TestTask{" +
                "state=" + state +
                ", latch=" + latch +
                ", failException=" + failException +
                ", id=" + id +
                ", name='" + name + '\'' +
                ", events=" + events +
                ", thread history=" + thread_history +
                '}';
    }
}
