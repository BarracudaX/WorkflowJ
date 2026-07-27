package com.barracuda.engine.builder;

import com.barracuda.engine.chain.ActionTaskNode;
import com.barracuda.engine.chain.ChainNode;
import com.barracuda.engine.chain.ParallelNode;
import com.barracuda.engine.chain.DataTaskNode;
import com.barracuda.engine.event.FlowEventPublisher;
import com.barracuda.engine.event.NoOpEvenPublisher;
import com.barracuda.engine.event.SubflowEventPublisherDecorator;
import com.barracuda.engine.flow.Flow;
import com.barracuda.engine.flow.SubflowDecorator;
import com.barracuda.engine.task.ActionTask;
import com.barracuda.engine.task.DataTask;

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

    public <I, R> T ioDataTask(DataTask<I, R> task, Supplier<I> inputSupplier, Consumer<R> outputConsumer) {
        chainNodes.add( (next) -> new DataTaskNode<>(next,task,inputSupplier,outputConsumer, ioExecutor));
        return self();
    }

    public <I, R> T cpuDataTask(DataTask<I, R> task, Supplier<I> inputSupplier, Consumer<R> outputConsumer) {
        chainNodes.add( (next) -> new DataTaskNode<>(next,task,inputSupplier,outputConsumer,cpuExecutor));
        return self();
    }

    public <I, R> T cpuDataTask(DataTask<I, R> task) {
        return cpuDataTask(task, nullSupplier(), noopConsumer());
    }

    public <I, R> T ioDataTask(DataTask<I, R> task) {
        return ioDataTask(task, nullSupplier(), noopConsumer());
    }

    public T actionTask(ActionTask actionTask) {
        chainNodes.add((next) -> new ActionTaskNode(actionTask, next, ioExecutor));
        return self();
    }

    public <I,R> T ioDataTask(DataTask<I,R> task, Supplier<I> supplier){
        return ioDataTask(task,supplier,noopConsumer());
    }

    public T runnableTask(Runnable task, long id) {
        return actionTask(ActionTask.fromRunnable(task,id));
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
        private Consumer<Flow> buildHook = _ -> {};

        public SubflowBuilder() {
        }

        public SubflowBuilder subflow(long subflowID, Consumer<AbstractFlowBuilder<?>> flowBuilderConsumer) {
            var builder = new FlowBuilder(subflowID, cpuExecutor, ioExecutor,rootID).eventPublisher(new SubflowEventPublisherDecorator(subflowID,rootID,flowEventPublisher));
            flowBuilderConsumer.accept(builder);

            Flow subflow = new SubflowDecorator(builder.build());
            buildHook.accept(subflow);
            subflows.add(subflow);

            return this;
        }

        public void onBuild(Consumer<Flow> buildHook) {
            this.buildHook = Objects.requireNonNull(buildHook);
        }
    }
}
