package com.barracuda.engine.chain;

import com.barracuda.engine.event.Command;
import com.barracuda.engine.event.Command.Continue;
import com.barracuda.engine.event.Command.Prepare;
import com.barracuda.engine.event.Command.Reset;
import com.barracuda.engine.event.ExecutionEvent;
import com.barracuda.engine.event.ExecutionEvent.TaskEvent.TaskStartEvent;
import com.barracuda.engine.event.FlowEventPublisher;
import com.barracuda.engine.flow.FlowInterruptedException;

import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static com.barracuda.engine.flow.Flow.FLOW_CONTEXT;

public abstract class AbstractTaskNode implements ChainNode{

    protected final ChainNode next;
    private volatile boolean havePublishedTaskStartedEvent  = false;

    protected AbstractTaskNode(ChainNode next) {
        this.next = next;
    }


    protected abstract void executeTask() throws ExecutionException, InterruptedException ;

    protected abstract long taskID();

    @Override
    public void command(Command command) {
        switch (command) {
            case Continue continueCommand -> handleContinueCommand(continueCommand);
            case Prepare _, Reset _ -> propagateCommand(command);
        }
    }

    private void handleContinueCommand(Continue continueCommand) {

        TaskStartEvent taskStartedEvent = new TaskStartEvent(FLOW_CONTEXT.get().flowID(), taskID());
        FlowEventPublisher eventPublisher = FLOW_CONTEXT.get().flowEventPublisher();
        long flowID = FLOW_CONTEXT.get().flowID();

        if (!havePublishedTaskStartedEvent) {
            eventPublisher.publish(taskStartedEvent);
        }

        event(taskStartedEvent);

        try {
            executeTask();
        } catch (Exception ex){
            handle(ex,flowID);
        }

        ExecutionEvent.TaskEvent.TaskCompletedEvent taskCompletedEvent = new ExecutionEvent.TaskEvent.TaskCompletedEvent(flowID, taskID());
        eventPublisher.publish(taskCompletedEvent);
        event(taskCompletedEvent);

        propagateCommand(continueCommand);
    }

    @Override
    public void event(ExecutionEvent event) {
        if (Objects.requireNonNull(event) instanceof ExecutionEvent.TaskEvent ev && ev.taskID() == taskID()) {
            taskEvent(ev);
        } else {
            if (next != null) {
                next.event(event);
            }
        }
    }

    private void taskEvent(ExecutionEvent.TaskEvent taskEvent) {
        switch (taskEvent) {
            case TaskStartEvent ev -> taskStartedEvent(ev);
            case ExecutionEvent.TaskEvent.TaskFailedEvent ev -> taskFailedEvent(ev);
            case ExecutionEvent.TaskEvent.TaskPausedEvent ev -> taskPausedEvent(ev);
            case ExecutionEvent.TaskEvent.TaskCompletedEvent ev -> {}
            case ExecutionEvent.TaskEvent.TaskResetEvent ev -> taskResetEvent(ev);
        }
    }

    private void taskResetEvent(ExecutionEvent.TaskEvent.TaskResetEvent taskResetEvent) {
        havePublishedTaskStartedEvent = false;
    }

    private void taskPausedEvent(ExecutionEvent event) {

    }

    private void taskFailedEvent(ExecutionEvent.TaskEvent.TaskFailedEvent taskFailedEvent) {
        throw taskFailedEvent.exception();
    }

    private void taskStartedEvent(TaskStartEvent taskStartEvent) {
        havePublishedTaskStartedEvent = true;
    }

    private void propagateCommand(Command command) {
        if (next != null) {
            next.command(command);
        }
    }

    private void handle(Throwable cause,long flowID) {
        switch (cause){
            case ExecutionException ex -> handle(ex.getCause(),flowID);
            case FlowInterruptedException ex -> {
                Thread.currentThread().interrupt();

                ExecutionEvent.TaskEvent.TaskPausedEvent taskpausedEvent = new ExecutionEvent.TaskEvent.TaskPausedEvent(flowID, taskID());

                FLOW_CONTEXT.get().flowEventPublisher().publish(taskpausedEvent);

                event(taskpausedEvent);

                throw ex;
            }
            case InterruptedException ex -> handle(new FlowInterruptedException("Task Interrupted",ex), flowID);
            case RuntimeException ex -> {
                ExecutionEvent.TaskEvent.TaskFailedEvent taskFailedEvent = new ExecutionEvent.TaskEvent.TaskFailedEvent(flowID, taskID(), ex);
                FLOW_CONTEXT.get().flowEventPublisher().publish(taskFailedEvent);
                event(taskFailedEvent);
            }
            default -> handle(new RuntimeException(cause),flowID);
        }
    }

}
