package com.barracuda.engine.chain;

import com.barracuda.engine.event.Command;
import com.barracuda.engine.event.Command.Continue;
import com.barracuda.engine.event.Command.Prepare;
import com.barracuda.engine.event.Command.Reset;
import com.barracuda.engine.event.ExecutionEvent;
import com.barracuda.engine.flow.FlowPrettyOutput;
import com.barracuda.engine.task.ActionTask;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class ActionTaskNode extends AbstractTaskNode{

    private final ActionTask task;
    private final ExecutorService executor;

    public ActionTaskNode(ActionTask task, ChainNode next, ExecutorService executor) {
        super(next);
        this.task = task;
        this.executor = executor;
    }


    @Override
    public void prettyPrint(FlowPrettyOutput output) {
        output.increaseLevel();
        StringBuilder sb = output.getStringBuilder();
        sb
                .append("\n").append(output.getTab()).append("[Action Task Node]")
                .append("\n").append(output.getTab()).append("Task Details: ")
                .append("\n").append(output.getTab()).append(task.toString())
                .append("\n\n");

        if (next != null) {
            sb.append(output.getTab()).append("Next Node:");
            next.prettyPrint(output);
        }
        output.decreaseLevel();
    }

    @Override
    protected void executeTask() throws ExecutionException, InterruptedException {
        Future<?> taskResult = null;
        try{
            taskResult = executor.submit(task::execute);
            taskResult.get();
        }finally {
            if (taskResult != null) {
                taskResult.cancel(true);
            }
        }
    }

    @Override
    protected long taskID() {
        return task.id();
    }
}
