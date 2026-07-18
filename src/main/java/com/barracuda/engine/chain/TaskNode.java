package com.barracuda.engine.chain;

import com.barracuda.engine.task.Task;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class TaskNode<I,R> implements ChainNode{

    private final ChainNode next;
    private final Task<I,R> task;
    private final Supplier<I> taskInputSupplier;
    private final Consumer<R> taskOutputConsumer;
    private final ExecutorService executor;

    public TaskNode(ChainNode next, Task<I, R> task, Supplier<I> taskInputSupplier, Consumer<R> taskOutputConsumer, ExecutorService executor) {
        this.next = next;
        this.task = task;
        this.taskInputSupplier = taskInputSupplier;
        this.taskOutputConsumer = taskOutputConsumer;
        this.executor = executor;
    }

    @Override
    public void execute() {

        I input = taskInputSupplier.get();

        R output = null;
        try {
            output = executor.submit(() -> task.execute(input)).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

        taskOutputConsumer.accept(output);

        if (next != null) {
            next.execute();
        }

    }

}
