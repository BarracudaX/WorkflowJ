package com.barracuda.engine.test.task;

import com.barracuda.engine.task.DataTask;

import java.util.concurrent.CountDownLatch;

public record ParallelTestTask(CountDownLatch notifyReadyLatch, CountDownLatch barrierLatch, long id) implements DataTask<Void, Void> {

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
