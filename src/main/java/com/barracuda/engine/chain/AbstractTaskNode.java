package com.barracuda.engine.chain;

import com.barracuda.engine.command.Command;
import com.barracuda.engine.command.Command.Continue;
import com.barracuda.engine.command.Command.Reset;
import com.barracuda.engine.command.CommandRejectedException;
import com.barracuda.engine.event.ExecutionEvent;
import com.barracuda.engine.event.ExecutionEvent.TaskEvent.*;
import com.barracuda.engine.event.FlowEventPublisher;
import com.barracuda.engine.flow.FlowInterruptedException;
import com.barracuda.engine.task.TaskStatus;

import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static com.barracuda.engine.flow.Flow.FLOW_CONTEXT;

public abstract class AbstractTaskNode implements ChainNode{

    protected final ChainNode next;
    private volatile TaskStatus status = TaskStatus.READY;

    protected AbstractTaskNode(ChainNode next) {
        this.next = next;
    }


    protected abstract void executeTask() throws ExecutionException, InterruptedException ;

    protected abstract long taskID();

    @Override
    public void command(Command command) {
        if(status == TaskStatus.COMPLETED) {
            next.command(command);
            return;
        }
        switch (command) {
            case Continue continueCommand -> handleContinueCommand(continueCommand);
            case Reset resetCommand -> handleResetCommand(resetCommand);
        }
    }

    private void handleResetCommand(Reset command) {
        if (status == TaskStatus.RUNNING) {
            throw new CommandRejectedException("Cannot reset a running task.");
        }

        next.command(command);

        taskReset();
    }

    private void taskReset(){
        TaskResetEvent taskResetEvent = new TaskResetEvent(FLOW_CONTEXT.get().flowID(), taskID());

        FLOW_CONTEXT.get().flowEventPublisher().publish(taskResetEvent);

        taskEvent(taskResetEvent);
    }

    private void handleContinueCommand(Continue continueCommand) {
        if (status != TaskStatus.READY) {
            throw new CommandRejectedException("Task cannot continue due to its state being " + status);
        }

        taskStarting();

        try {
            executeTask();
        } catch (Exception ex){
            taskFailed(ex,FLOW_CONTEXT.get().flowID());
        }

        taskCompleted();

        next.command(continueCommand);
    }

    private void taskCompleted() {
        FlowEventPublisher eventPublisher = FLOW_CONTEXT.get().flowEventPublisher();

        TaskCompletedEvent taskCompletedEvent = new TaskCompletedEvent(FLOW_CONTEXT.get().flowID(), taskID());

        eventPublisher.publish(taskCompletedEvent);

        event(taskCompletedEvent);
    }

    private void taskStarting(){
        FlowEventPublisher eventPublisher = FLOW_CONTEXT.get().flowEventPublisher();

        TaskStartEvent taskStartedEvent = new TaskStartEvent(FLOW_CONTEXT.get().flowID(), taskID());

        eventPublisher.publish(taskStartedEvent);

        event(taskStartedEvent);
    }

    @Override
    public void event(ExecutionEvent event) {
        if (Objects.requireNonNull(event) instanceof ExecutionEvent.TaskEvent ev && ev.taskID() == taskID() && ev.flowID() == FLOW_CONTEXT.get().flowID()) {
            taskEvent(ev);
        } else {
            next.event(event);
        }
    }

    private void taskEvent(ExecutionEvent.TaskEvent taskEvent) {
        switch (taskEvent) {
            case TaskStartEvent _ -> taskStartedEvent();
            case TaskFailedEvent ev -> taskFailedEvent(ev);
            case TaskPausedEvent _ -> taskPausedEvent();
            case TaskCompletedEvent _ -> taskCompletedEvent();
            case TaskResetEvent _ -> taskResetEvent();
        }
    }

    private void taskCompletedEvent() {
        status = TaskStatus.COMPLETED;
    }

    private void taskResetEvent() {
        status = TaskStatus.READY;
    }

    private void taskPausedEvent() {
        status = TaskStatus.PAUSED;
    }

    private void taskFailedEvent(TaskFailedEvent taskFailedEvent) {
        status = TaskStatus.FAILED;
        throw taskFailedEvent.exception();
    }

    private void taskStartedEvent() {
        status = TaskStatus.RUNNING;
    }

    private void taskFailed(Throwable cause, long flowID) {
        switch (cause){
            case ExecutionException ex -> taskFailed(ex.getCause(),flowID);
            case FlowInterruptedException ex -> {
                Thread.currentThread().interrupt();

                TaskPausedEvent taskpausedEvent = new TaskPausedEvent(flowID, taskID());

                FLOW_CONTEXT.get().flowEventPublisher().publish(taskpausedEvent);

                event(taskpausedEvent);

                throw ex;
            }
            case InterruptedException ex -> taskFailed(new FlowInterruptedException("Task Interrupted",ex), flowID);
            case RuntimeException ex -> {
                TaskFailedEvent taskFailedEvent = new TaskFailedEvent(flowID, taskID(), ex);
                FLOW_CONTEXT.get().flowEventPublisher().publish(taskFailedEvent);
                event(taskFailedEvent);
            }
            default -> taskFailed(new RuntimeException(cause),flowID);
        }
    }

}
