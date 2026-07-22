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
    private final long rootID;

    ParallelChainNodeBuilder(ExecutorService cpuExecutor, ExecutorService ioExecutor, FlowEventPublisher flowEventPublisher, long rootID) {
        this.cpuExecutor = cpuExecutor;
        this.ioExecutor = ioExecutor;
        this.flowEventPublisher = flowEventPublisher;
        this.rootID = rootID;
    }

    public ParallelChainNodeBuilder subflow(Consumer<FlowBuilder<?>> flowBuilderConsumer) {
        var builder = new RootFlowBuilder(cpuExecutor, ioExecutor,flowEventPublisher,rootID);
        flowBuilderConsumer.accept(builder);

        subflows.add(builder);

        return this;
    }

}
