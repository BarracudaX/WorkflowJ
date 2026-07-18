package com.barracuda.engine.task;

public interface Task<I, R> {

    static Task<Void,Void> fromRunnable(Runnable runnable) {
        return (_) -> {
            runnable.run();
            return null;
        };
    }

    R execute(I input);

}
