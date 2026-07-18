package com.barracuda.engine.flow;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public class ParallelChainNodeBuilder {

    private final ExecutorService cpuExecutor;
    private final ExecutorService ioExecutor;
    final List<RootFlowBuilder> subflows = new ArrayList<>();

    public ParallelChainNodeBuilder(ExecutorService cpuExecutor, ExecutorService ioExecutor) {
        this.cpuExecutor = cpuExecutor;
        this.ioExecutor = ioExecutor;
    }

    public ParallelChainNodeBuilder subflow(Consumer<FlowBuilder<?>> flowBuilderConsumer) {
        var builder = new RootFlowBuilder(cpuExecutor, ioExecutor);
        flowBuilderConsumer.accept(builder);

        subflows.add(builder);

        return this;
    }

}
