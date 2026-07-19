package com.barracuda.engine.builder;

import com.barracuda.engine.event.FlowEventPublisher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public class ParallelChainNodeBuilder {

    private final ExecutorService cpuExecutor;
    private final ExecutorService ioExecutor;
    final List<RootFlowBuilder> subflows = new ArrayList<>();
    private final FlowEventPublisher  flowEventPublisher;

    public ParallelChainNodeBuilder(ExecutorService cpuExecutor, ExecutorService ioExecutor, FlowEventPublisher flowEventPublisher) {
        this.cpuExecutor = cpuExecutor;
        this.ioExecutor = ioExecutor;
        this.flowEventPublisher = flowEventPublisher;
    }

    public ParallelChainNodeBuilder subflow(Consumer<FlowBuilder<?>> flowBuilderConsumer) {
        var builder = new RootFlowBuilder(cpuExecutor, ioExecutor,flowEventPublisher);
        flowBuilderConsumer.accept(builder);

        subflows.add(builder);

        return this;
    }

}
