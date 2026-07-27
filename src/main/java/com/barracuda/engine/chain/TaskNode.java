package com.barracuda.engine.chain;

import com.barracuda.engine.event.Command;
import com.barracuda.engine.event.Command.Continue;
import com.barracuda.engine.event.Command.Prepare;
import com.barracuda.engine.event.Command.Reset;
import com.barracuda.engine.event.ExecutionEvent;
import com.barracuda.engine.event.ExecutionEvent.TaskEvent;
import com.barracuda.engine.event.ExecutionEvent.TaskEvent.*;
import com.barracuda.engine.event.FlowEventPublisher;
import com.barracuda.engine.flow.FlowInterruptedException;
import com.barracuda.engine.flow.FlowPrettyOutput;
import com.barracuda.engine.task.Task;

import java.util.Objects;
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
    private volatile boolean havePublishedTaskStartedEvent  = false;

    public TaskNode(ChainNode next, Task<I, R> task, Supplier<I> taskInputSupplier, Consumer<R> taskOutputConsumer, ExecutorService executor) {
        this.next = next;
        this.task = task;
        this.taskInputSupplier = taskInputSupplier;
        this.taskOutputConsumer = taskOutputConsumer;
        this.executor = executor;
    }


    @Override
    public void command(Command command) {
        switch (command) {
            case Continue continueCommand -> handleContinueCommand(continueCommand);
            case Prepare _, Reset _ -> propagateCommand(command);
        }
    }

    private void handleContinueCommand(Continue continueCommand) {

        TaskStartEvent taskStartedEvent = new TaskStartEvent(FLOW_CONTEXT.get().flowID(), task.id());
        FlowEventPublisher eventPublisher = FLOW_CONTEXT.get().flowEventPublisher();

        if (!havePublishedTaskStartedEvent) {
            eventPublisher.publish(taskStartedEvent);
        }

        event(taskStartedEvent);

        executeTask();

        propagateCommand(continueCommand);
    }

    private void executeTask(){
        FlowEventPublisher eventPublisher = FLOW_CONTEXT.get().flowEventPublisher();
        long flowID = FLOW_CONTEXT.get().flowID();

        I input = taskInputSupplier.get();
        Future<R> taskResult = null;
        R result = null;
        try {
            taskResult = executor.submit(() -> task.execute(input));
            result = taskResult.get();
        } catch (Exception ex) {
            if (taskResult != null) {
                taskResult.cancel(true);
            }
            handle(ex,flowID);
        }

        taskOutputConsumer.accept(result);

        TaskCompletedEvent taskCompletedEvent = new TaskCompletedEvent(flowID, task.id());
        eventPublisher.publish(taskCompletedEvent);
        event(taskCompletedEvent);
    }

    private void propagateCommand(Command command) {
        if (next != null) {
            next.command(command);
        }
    }


    @Override
    public void event(ExecutionEvent event) {
        if (Objects.requireNonNull(event) instanceof TaskEvent ev && ev.taskID() == task.id()) {
            taskEvent(ev);
        } else {
            if (next != null) {
                next.event(event);
            }
        }
    }

    private void taskEvent(TaskEvent taskEvent) {
        switch (taskEvent) {
            case TaskStartEvent ev -> taskStartedEvent(ev);
            case TaskFailedEvent ev -> taskFailedEvent(ev);
            case TaskPausedEvent ev -> taskPausedEvent(ev);
            case TaskCompletedEvent ev -> {}
            case TaskResetEvent ev -> taskResetEvent(ev);
        }
    }

    private void taskResetEvent(TaskResetEvent taskResetEvent) {
        havePublishedTaskStartedEvent = false;
    }

    private void taskPausedEvent(ExecutionEvent event) {

    }

    private void taskFailedEvent(TaskFailedEvent taskFailedEvent) {
        throw taskFailedEvent.exception();
    }

    private void taskStartedEvent(TaskStartEvent taskStartEvent) {
        havePublishedTaskStartedEvent = true;
    }

    @Override
    public void prettyPrint(FlowPrettyOutput output) {
        output.increaseLevel();
        StringBuilder sb = output.getStringBuilder();
        sb
                .append("\n").append(output.getTab()).append("[Task Node]")
                .append("\n").append(output.getTab()).append("Task Details: ")
                .append("\n").append(output.getTab()).append(task.toString())
                .append("\n").append(output.getTab()).append("Task Input Loader: ")
                .append("\n").append(output.getTab()).append(taskInputSupplier.toString())
                .append("\n").append(output.getTab()).append("Task Output Consumer: ")
                .append("\n").append(output.getTab()).append(taskOutputConsumer.toString()).append("\n\n");

        if (next != null) {
            sb.append(output.getTab()).append("Next Node:");
            next.prettyPrint(output);
        }
        output.decreaseLevel();
    }

    private void handle(Throwable cause,long flowID) {
        switch (cause){
            case ExecutionException ex -> handle(ex.getCause(),flowID);
            case FlowInterruptedException ex -> {
                Thread.currentThread().interrupt();

                TaskPausedEvent taskpausedEvent = new TaskPausedEvent(flowID, task.id());

                FLOW_CONTEXT.get().flowEventPublisher().publish(taskpausedEvent);

                event(taskpausedEvent);

                throw ex;
            }
            case InterruptedException ex -> handle(new FlowInterruptedException("Task Interrupted",ex), flowID);
            case RuntimeException ex -> {
                TaskFailedEvent taskFailedEvent = new TaskFailedEvent(flowID, task.id(), ex);
                FLOW_CONTEXT.get().flowEventPublisher().publish(taskFailedEvent);
                event(taskFailedEvent);
            }
            default -> handle(new RuntimeException(cause),flowID);
        }
    }

}
