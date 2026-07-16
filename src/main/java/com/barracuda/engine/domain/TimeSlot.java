package com.barracuda.engine.domain;

import java.time.Duration;

public record TimeSlot(Duration duration, long startPointNano) {

    public TimeSlot(Duration duration) {
        this(duration,System.nanoTime());
    }

    public boolean hasExpired(){

        return System.nanoTime() - startPointNano > duration.toNanos();

    }

}
