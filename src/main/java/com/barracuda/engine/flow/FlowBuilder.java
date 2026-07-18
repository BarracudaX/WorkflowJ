package com.barracuda.engine.flow;

import com.barracuda.engine.chain.ChainNode;
import com.barracuda.engine.chain.ParallelNode;
import com.barracuda.engine.chain.TaskNode;
import com.barracuda.engine.task.Task;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class FlowBuilder<T extends FlowBuilder<T>> {

    private final ExecutorService cpuExecutor;
    private final ExecutorService ioExecutor;
    protected final List<Function<ChainNode,ChainNode>> chainNodes = new ArrayList<>();

    protected FlowBuilder(ExecutorService cpuExecutor,ExecutorService ioExecutor) {
        this.cpuExecutor = cpuExecutor;
        this.ioExecutor = ioExecutor;
    }

    public T parallel(Consumer<ParallelChainNodeBuilder> consumer) {
        var builder = new ParallelChainNodeBuilder(cpuExecutor,ioExecutor);

        consumer.accept(builder);

        List<Flow> subflows = builder.subflows.stream().map(RootFlowBuilder::build).toList();

        chainNodes.add((next) -> new ParallelNode(subflows,next));

        return self();
    }

    public <I, R> T ioTask(Task<I, R> task, Supplier<I> inputSupplier, Consumer<R> outputConsumer) {
        chainNodes.add( (next) -> new TaskNode<>(next,task,inputSupplier,outputConsumer, ioExecutor));
        return self();
    }

    public <I, R> T cpuTask(Task<I, R> task, Supplier<I> inputSupplier, Consumer<R> outputConsumer) {
        chainNodes.add( (next) -> new TaskNode<>(next,task,inputSupplier,outputConsumer,cpuExecutor));
        return self();
    }

    public <I, R> T cpuTask(Task<I, R> task) {
        return cpuTask(task, nullSupplier(), noopConsumer());
    }

    public <I, R> T ioTask(Task<I, R> task) {
        return ioTask(task, nullSupplier(), noopConsumer());
    }

    /**
     * Configures a task with default input provider(provides null) and default output consumer(does nothing with the result).
     * This method is useful for tasks that have no input and produce no output. For example, for test tasks or for tasks that do execute deterministic code(do not rely on any input) with side effects.
     * This method shouldn't be used unless absolutely needed. Task should be pure function that depend on their input and produce output.
     * @param task the configured task
     * @return this builder
     */
    public T runnableTask(Runnable task) {
        return ioTask(Task.fromRunnable(task), nullSupplier(), noopConsumer());
    }

    private static <T> Consumer<T> noopConsumer(){
        return _ -> {};
    }

    private static <T>Supplier<T> nullSupplier(){
        return () -> (T) null;
    }

    protected abstract T self();
}
