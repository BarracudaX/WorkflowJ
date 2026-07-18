package com.barracuda.engine.chain;

import com.barracuda.engine.task.Task;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class CpuTaskNode<I, R> extends AbstractTaskNode<I, R> {

    public CpuTaskNode(ChainNode next, Task<I, R> task, Supplier<I> taskInputSupplier, Consumer<R> taskOutputConsumer, ExecutorService executor) {
        super(next, task, taskInputSupplier, taskOutputConsumer, executor);
    }

}
