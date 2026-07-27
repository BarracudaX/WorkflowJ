package com.barracuda.engine.task;

public interface ActionTask extends Task {

    static ActionTask fromRunnable(Runnable runnable, long id) {

        return new ActionTask() {

            @Override
            public void execute() {
                runnable.run();
            }

            @Override
            public long id() {
                return id;
            }
        };
    }

    void execute();

}
