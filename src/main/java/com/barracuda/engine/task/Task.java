package com.barracuda.engine.task;

public interface Task<I, R> {

    R execute(I input);

}
