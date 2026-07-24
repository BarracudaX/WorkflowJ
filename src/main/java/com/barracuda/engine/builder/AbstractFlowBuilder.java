package com.barracuda.engine.builder;

import com.barracuda.engine.chain.ChainNode;
import com.barracuda.engine.chain.ParallelNode;
import com.barracuda.engine.chain.TaskNode;
import com.barracuda.engine.event.FlowEventPublisher;
import com.barracuda.engine.event.NoOpEvenPublisher;
import com.barracuda.engine.event.SubflowEventPublisherDecorator;
import com.barracuda.engine.flow.Flow;
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
    protected FlowEventPublisher flowEventPublisher = new NoOpEvenPublisher();
    protected final long flowID;
    private final long rootID;

    protected AbstractFlowBuilder(long flowID, ExecutorService cpuExecutor, ExecutorService ioExecutor,long rootID) {
        this.cpuExecutor = cpuExecutor;
        this.ioExecutor = ioExecutor;
        this.flowID = flowID;
        this.rootID = rootID;
    }

    public T eventPublisher(FlowEventPublisher flowEventPublisher) {
        this.flowEventPublisher = flowEventPublisher;
        return self();
    }

    public T parallel(Consumer<SubflowBuilder> consumer) {
        var builder = new SubflowBuilder();

        consumer.accept(builder);

        chainNodes.add((next) -> new ParallelNode(builder.subflows,next));

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

    public class SubflowBuilder {

        final List<Flow> subflows = new ArrayList<>();

        public SubflowBuilder() {
        }

        public SubflowBuilder subflow(long subflowID, Consumer<AbstractFlowBuilder<?>> flowBuilderConsumer) {
            var builder = new FlowBuilder(subflowID, cpuExecutor, ioExecutor,rootID).eventPublisher(new SubflowEventPublisherDecorator(subflowID,rootID,flowEventPublisher));
            flowBuilderConsumer.accept(builder);

            subflows.add(builder.build());

            return this;
        }

    }
}
