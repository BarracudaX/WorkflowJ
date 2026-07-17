package com.barracuda.engine.test;

import com.barracuda.engine.work.Work;
import lombok.Getter;

public class FailWork implements Work {

    @Getter
    private final RuntimeException exception;
    private final long id;

    public FailWork(RuntimeException exception, long id) {
        this.exception = exception;
        this.id = id;
    }

    @Override
    public void execute() {
        throw exception;
    }

    @Override
    public long id() {
        return id;
    }

}
