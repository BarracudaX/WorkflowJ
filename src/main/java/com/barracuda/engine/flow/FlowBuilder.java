package com.barracuda.engine.flow;

import com.barracuda.engine.chain.ChainNode;
import com.barracuda.engine.chain.AbstractTaskNode;
import com.barracuda.engine.chain.CpuTaskNode;
import com.barracuda.engine.chain.IoTaskNode;
import com.barracuda.engine.task.Task;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class FlowBuilder<T extends FlowBuilder<T>> {

    private final ExecutorService cpuExecutor;
    private final ExecutorService virtualThreadExecutor;
    protected final List<Function<ChainNode,ChainNode>> chainNodes = new ArrayList<>();

    protected FlowBuilder(ExecutorService cpuExecutor,ExecutorService virtualThreadExecutor) {
        this.cpuExecutor = cpuExecutor;
        this.virtualThreadExecutor = virtualThreadExecutor;
    }

    public <I, R> T ioTask(Task<I, R> task, Supplier<I> inputSupplier, Consumer<R> outputConsumer) {
        chainNodes.add( (next) -> new IoTaskNode<>(next,task,inputSupplier,outputConsumer,virtualThreadExecutor));
        return self();
    }

    public <I, R> T cpuTask(Task<I, R> task, Supplier<I> inputSupplier, Consumer<R> outputConsumer) {
        chainNodes.add( (next) -> new CpuTaskNode<>(next,task,inputSupplier,outputConsumer,cpuExecutor));
        return self();
    }

    public <I, R> T cpuTask(Task<I, R> task) {
        cpuTask(task, nullSupplier(), noopConsumer());
        return self();
    }

    public <I, R> T ioTask(Task<I, R> task) {
        ioTask(task, nullSupplier(), noopConsumer());
        return self();
    }

    /**
     * Configures a task with default input provider(provides null) and default output consumer(does nothing with the result).
     * This method is useful for tasks that have no input and produce no output. For example, for test tasks or for tasks that do execute deterministic code(do not rely on any input) with side effects.
     * This method shouldn't be used unless absolutely needed. Task should be pure function that depend on their input and produce output.
     * @param task the configured task
     * @return this builder
     */
    public T runnableTask(Runnable task) {
        ioTask(Task.fromRunnable(task), nullSupplier(), noopConsumer());
        return self();
    }

    private static <T> Consumer<T> noopConsumer(){
        return _ -> {};
    }

    private static <T>Supplier<T> nullSupplier(){
        return () -> (T) null;
    }

    protected abstract T self();
}
