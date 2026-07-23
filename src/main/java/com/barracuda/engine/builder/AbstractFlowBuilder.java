package com.barracuda.engine.builder;

import com.barracuda.engine.chain.ChainNode;
import com.barracuda.engine.chain.ParallelNode;
import com.barracuda.engine.chain.TaskNode;
import com.barracuda.engine.event.FlowEventPublisher;
import com.barracuda.engine.flow.Flow;
import com.barracuda.engine.flow.SubflowDecorator;
import com.barracuda.engine.task.Task;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class AbstractFlowBuilder<T extends AbstractFlowBuilder<T>> {

    private final ExecutorService cpuExecutor;
    private final ExecutorService ioExecutor;
    protected final List<Function<ChainNode,ChainNode>> chainNodes = new ArrayList<>();
    protected FlowEventPublisher flowEventPublisher;
    protected final long flowID;
    protected final Long rootID;

    protected AbstractFlowBuilder(long flowID, ExecutorService cpuExecutor, ExecutorService ioExecutor, FlowEventPublisher flowEventPublisher, Long rootID) {
        this.cpuExecutor = cpuExecutor;
        this.ioExecutor = ioExecutor;
        this.flowEventPublisher = flowEventPublisher;
        this.rootID = rootID;
        this.flowID = flowID;
    }

    public T parallel(Consumer<SubflowBuilder> consumer) {
        Objects.requireNonNull(rootID,"Cannot create subflows before root flow has an id");
        var builder = new SubflowBuilder(cpuExecutor,ioExecutor,flowEventPublisher,rootID);

        consumer.accept(builder);

        List<Flow> subflows = builder.subflows.stream()
                .map(FlowBuilder::build).map(flow -> (Flow) new SubflowDecorator(flow))
                .toList();

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

    public <I,R> T ioTask(Task<I,R> task, Supplier<I> supplier){
        return ioTask(task,supplier,noopConsumer());
    }

    /**
     * Configures a task with default input provider(provides null) and default output consumer(does nothing with the result).
     * This method is useful for tasks that have no input and produce no output. For example, for test tasks or for tasks that do execute deterministic code(do not rely on any input) with side effects.
     * This method shouldn't be used unless absolutely needed. Task should be pure function that depend on their input and produce output.
     * @param task the configured task
     * @return this builder
     */
    public T runnableTask(Runnable task, long id) {
        return ioTask(Task.fromRunnable(task,id), nullSupplier(), noopConsumer());
    }

    public static <T> Consumer<T> noopConsumer(){
        return _ -> {};
    }

    public static <T>Supplier<T> nullSupplier(){
        return () -> (T) null;
    }

    protected abstract T self();
}
