package com.barracuda.engine.chain;

import com.barracuda.engine.task.Task;

import java.util.function.Consumer;
import java.util.function.Supplier;

public class TaskChainNode<I,R> implements ChainNode{

    private final ChainNode next;
    private final Task<I,R> task;
    private final Supplier<I> taskInputSupplier;
    private final Consumer<R> taskOutputConsumer;

    public TaskChainNode(ChainNode next, Task<I, R> task, Supplier<I> taskInputSupplier, Consumer<R> taskOutputConsumer) {
        this.next = next;
        this.task = task;
        this.taskInputSupplier = taskInputSupplier;
        this.taskOutputConsumer = taskOutputConsumer;
    }

    @Override
    public void execute() {

        I input = taskInputSupplier.get();

        R output = task.execute(input);

        taskOutputConsumer.accept(output);

        if (next != null) {
            next.execute();
        }
    }

}
