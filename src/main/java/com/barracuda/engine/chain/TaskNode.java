package com.barracuda.engine.chain;

import com.barracuda.engine.event.ExecutionEvent;
import com.barracuda.engine.event.ExecutionEvent.TaskEvent.TaskFailedEvent;
import com.barracuda.engine.event.ExecutionEvent.TaskEvent.TaskPausedEvent;
import com.barracuda.engine.event.ExecutionEvent.TaskEvent.TaskStartedEvent;
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
        FLOW_CONTEXT.get().getFlowEventPublisher().publish(new TaskStartedEvent(task.id()));

        I input = taskInputSupplier.get();

        Future<R> taskResult = null;
        R result = null;
        try {
             taskResult = executor.submit(() -> task.execute(input));
             result = taskResult.get();
        } catch (Exception ex) {
            handle(ex,taskResult);
        }

        FLOW_CONTEXT.get().getFlowEventPublisher().publish(new ExecutionEvent.TaskEvent.TaskCompletedEvent(task.id()));

        taskOutputConsumer.accept(result);

        if (next != null) {
            next.execute();
        }

    }

    private void handle(Throwable cause, Future<R> taskFuture) {
        switch (cause){
            case ExecutionException ex -> handle(ex.getCause(),taskFuture);
            case InterruptedException ex -> handleInterrupted(taskFuture, ex);
            case RuntimeException ex -> handleRuntimeException(ex);
            default -> handleRuntimeException(new RuntimeException(cause));
        }
    }

    private void handleInterrupted(Future<R> taskFuture, InterruptedException ex) {
        Thread.currentThread().interrupt();
        taskFuture.cancel(true);
        FLOW_CONTEXT.get().getFlowEventPublisher().publish(new TaskPausedEvent(task.id()));
        throw new FlowInterruptedException("Flow Interrupted", ex);
    }

    private void handleRuntimeException(RuntimeException ex) {
        FLOW_CONTEXT.get().getFlowEventPublisher().publish(new TaskFailedEvent(task.id(), ex));
        throw ex;
    }

}
