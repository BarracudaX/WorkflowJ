package com.barracuda.engine.task;

public interface Task<I, R> {

    static Task<Void,Void> fromRunnable(Runnable runnable,long id) {

        return new Task<>() {

            @Override
            public Void execute(Void input) {

                runnable.run();
                return null;
            }

            @Override
            public long id() {
                return id;
            }
        };
    }

    R execute(I input);

    long id();
}
