package com.barracuda.engine.test;

import com.barracuda.engine.work.Work;

public class FailWork implements Work {

    private final RuntimeException exception;

    public FailWork(RuntimeException exception) {
        this.exception = exception;
    }

    @Override
    public void execute() {
        throw exception;
    }

}
