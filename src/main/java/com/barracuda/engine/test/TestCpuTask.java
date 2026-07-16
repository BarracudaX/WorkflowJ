package com.barracuda.engine.test;

import com.barracuda.engine.domain.*;
import com.barracuda.engine.task.CpuTask;

import java.time.Duration;

public class TestCpuTask extends CpuTask {

    private long remainingNanos;

    public TestCpuTask(String name,long id,Duration duration) {
        super(name,id);
        this.remainingNanos = duration.toNanos();
    }

    @Override
    public void executeTask(TimeSlot timeSlot) throws TaskNeedMoreTimeException {

        double dummyValue = 0.12345;

        long loopStartTime = System.nanoTime();

        while (remainingNanos > 0 && !Thread.currentThread().isInterrupted()) {

            if(timeSlot.hasExpired()){
                remainingNanos -= (System.nanoTime() - loopStartTime);
                throw new TaskNeedMoreTimeException();
            }

            dummyValue = Math.sin(Math.cos(Math.tan(dummyValue))) + Math.log(dummyValue + 1.0);
            long now = System.nanoTime();
            remainingNanos -= (now - loopStartTime);
            loopStartTime = now;
        }

        if(Thread.currentThread().isInterrupted()){
            return ;
        }

        if (dummyValue == 999.999) {
            System.out.println("If this prints, you broke mathematics: " + dummyValue);
        }

    }

}
