package com.barracuda.engine.task;

public interface DataTask<I, R> extends Task {



    R execute(I input);
}
