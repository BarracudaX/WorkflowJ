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
import com.barracuda.engine.task.DataTask;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.barracuda.engine.flow.FlowImpl.FLOW_CONTEXT;

public class DataTaskNode<I,R> extends AbstractTaskNode{

    private final DataTask<I,R> task;
    private final Supplier<I> taskInputSupplier;
    private final Consumer<R> taskOutputConsumer;
    private final ExecutorService executor;

    public DataTaskNode(ChainNode next, DataTask<I, R> task, Supplier<I> taskInputSupplier, Consumer<R> taskOutputConsumer, ExecutorService executor) {
        super(next);
        this.task = task;
        this.taskInputSupplier = taskInputSupplier;
        this.taskOutputConsumer = taskOutputConsumer;
        this.executor = executor;
    }

    @Override
    protected void executeTask() throws ExecutionException, InterruptedException {
        FlowEventPublisher eventPublisher = FLOW_CONTEXT.get().flowEventPublisher();
        long flowID = FLOW_CONTEXT.get().flowID();

        I input = taskInputSupplier.get();
        Future<R> taskResult = null;
        R result = null;
        try {
            taskResult = executor.submit(() -> task.execute(input));
            result = taskResult.get();
        }finally {
            if(taskResult != null){
                taskResult.cancel(true);
            }
        }

        taskOutputConsumer.accept(result);
    }

    @Override
    protected long taskID() {
        return task.id();
    }

    @Override
    public void prettyPrint(FlowPrettyOutput output) {
        output.increaseLevel();
        StringBuilder sb = output.getStringBuilder();
        sb
                .append("\n").append(output.getTab()).append("[Data Task Node]")
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

}
