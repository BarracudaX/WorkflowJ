package com.barracuda.engine.chain;

import com.barracuda.engine.event.ExecutionEvent;
import com.barracuda.engine.event.ExecutionEvent.ContinueEvent;
import com.barracuda.engine.event.ExecutionEvent.TaskEvent.TaskCompletedEvent;
import com.barracuda.engine.event.ExecutionEvent.TaskEvent.TaskFailedEvent;
import com.barracuda.engine.event.ExecutionEvent.TaskEvent.TaskPausedEvent;
import com.barracuda.engine.event.ExecutionEvent.TaskEvent.TaskStartEvent;
import com.barracuda.engine.flow.FlowInterruptedException;
import com.barracuda.engine.task.Task;

import java.util.ConcurrentModificationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.barracuda.engine.flow.FlowImpl.FLOW_CONTEXT;

public class TaskNode<I,R> implements ChainNode{

    private final ChainNode next;
    private final Task<I,R> task;
    private final Supplier<I> taskInputSupplier;
    private final Consumer<R> taskOutputConsumer;
    private final ExecutorService executor;
    private final AtomicBoolean havePublishedTaskStartedEvent  = new AtomicBoolean(false);

    public TaskNode(ChainNode next, Task<I, R> task, Supplier<I> taskInputSupplier, Consumer<R> taskOutputConsumer, ExecutorService executor) {
        this.next = next;
        this.task = task;
        this.taskInputSupplier = taskInputSupplier;
        this.taskOutputConsumer = taskOutputConsumer;
        this.executor = executor;
    }

    @Override
    public void event(ExecutionEvent event) {
        //add more cases for supplier and consumer in the future.
        switch (event){
            case TaskFailedEvent ev when ev.taskID() == task.id() -> throw ev.exception();
            case TaskStartEvent ev when ev.taskID() == task.id() -> {
                if(!havePublishedTaskStartedEvent.compareAndSet(false, true)){
                    throw new ConcurrentModificationException("Flow potentially getting events from multiple sources");
                }
                return;
            }
            case ContinueEvent _ -> { }
            default -> {
                if(next != null){
                    next.event(event);
                }
                return;
            }
        }

        //should get to this point only if the event was the ContinueEvent.

        var flowID = FLOW_CONTEXT.get().flowID();
        var eventPublisher = FLOW_CONTEXT.get().flowEventPublisher();

        eventPublisher.publish(new TaskStartEvent(flowID,task.id()));

        I input = taskInputSupplier.get();

        Future<R> taskResult = null;
        R result = null;
        try {
             taskResult = executor.submit(() -> task.execute(input));
             result = taskResult.get();
        } catch (Exception ex) {
            handle(ex,taskResult,flowID);
        }

        eventPublisher.publish(new TaskCompletedEvent(flowID,task.id()));

        taskOutputConsumer.accept(result);

        if (next != null) {
            next.event(event);
        }

    }

    private void handle(Throwable cause, Future<R> taskFuture,long flowID) {
        switch (cause){
            case ExecutionException ex -> handle(ex.getCause(),taskFuture,flowID);
            case InterruptedException ex -> handleInterrupted(taskFuture, ex,flowID);
            case RuntimeException ex -> handleRuntimeException(ex,flowID);
            default -> handleRuntimeException(new RuntimeException(cause),flowID);
        }
    }

    private void handleInterrupted(Future<R> taskFuture, InterruptedException ex,long flowID) {
        Thread.currentThread().interrupt();
        taskFuture.cancel(true);
        FLOW_CONTEXT.get().flowEventPublisher().publish(new TaskPausedEvent(flowID,task.id()));
        throw new FlowInterruptedException("Flow Interrupted", ex);
    }

    private void handleRuntimeException(RuntimeException ex,long flowID) {
        FLOW_CONTEXT.get().flowEventPublisher().publish(new TaskFailedEvent(flowID,task.id(), ex));
        throw ex;
    }

}
