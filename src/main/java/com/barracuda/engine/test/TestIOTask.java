package com.barracuda.engine.test;

import com.barracuda.engine.domain.TaskResult;
import com.barracuda.engine.task.IOTask;
import lombok.ToString;

import java.time.Duration;

@ToString
public class TestIOTask extends IOTask {

    private final Duration duration;
    private final Duration sleep;

    public TestIOTask(Duration sleep,String name, long id, Duration duration) {
        super(name,id);
        this.duration = duration;
        this.sleep = sleep;
    }

    @Override
    public void executeTask() {
        work();
    }


    private TaskResult work() {

        long endTime = System.nanoTime() + duration.toNanos();

        while(System.nanoTime() < endTime && !Thread.currentThread().isInterrupted()){
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        if(System.nanoTime() >= endTime){
            return TaskResult.COMPLETED;
        }

        throw new IllegalStateException("Should never reach this point");

    }
}
