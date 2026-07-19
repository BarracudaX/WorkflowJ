package com.barracuda.engine.chain;

import com.barracuda.engine.event.FlowEvent.FlowTaskStartedEvent;
import com.barracuda.engine.flow.FlowInterruptedException;
import com.barracuda.engine.task.Task;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.barracuda.engine.flow.FlowImpl.FLOW_CONTEXT;

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
        FLOW_CONTEXT.get().getFlowEventPublisher().publish(new FlowTaskStartedEvent(task.id()));

        I input = taskInputSupplier.get();

        Future<R> taskResult = null;
        R result = null;
        try {
             taskResult = executor.submit(() -> task.execute(input));
             result = taskResult.get();
        } catch (Exception ex) {
            handle(ex,taskResult);
        }

        taskOutputConsumer.accept(result);

        if (next != null) {
            next.execute();
        }

    }

    private void handle(Throwable cause, Future<R> task) {
        switch (cause){
            case ExecutionException ex -> handle(ex.getCause(),task);
            case InterruptedException ex -> {
                Thread.currentThread().interrupt();
                task.cancel(true);
                throw new FlowInterruptedException("Flow Interrupted", ex);
            }
            case RuntimeException ex -> throw ex;
            default -> throw new RuntimeException(cause);
        }
    }

}
