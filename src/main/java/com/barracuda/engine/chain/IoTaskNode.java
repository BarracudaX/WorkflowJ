package com.barracuda.engine.chain;

import com.barracuda.engine.task.Task;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class IoTaskNode<I,R> extends AbstractTaskNode<I,R> {

    public IoTaskNode(ChainNode next, Task<I, R> task, Supplier<I> taskInputSupplier, Consumer<R> taskOutputConsumer, ExecutorService executor) {
        super(next, task, taskInputSupplier, taskOutputConsumer, executor);
    }

}
