package com.barracuda.engine.test.task;

import com.barracuda.engine.task.ActionTask;

import java.util.concurrent.CountDownLatch;

public record ParallelTestTask(CountDownLatch notifyReadyLatch, CountDownLatch barrierLatch, long id) implements ActionTask {

    @Override
    public void execute() {
        notifyReadyLatch.countDown();
        try {
            barrierLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
