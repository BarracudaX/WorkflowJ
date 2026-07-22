package com.barracuda.engine.builder;

import com.barracuda.engine.event.FlowEventPublisher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public class SubflowBuilder {

    private final ExecutorService cpuExecutor;
    private final ExecutorService ioExecutor;
    final List<FlowBuilder> subflows = new ArrayList<>();
    private final FlowEventPublisher  flowEventPublisher;
    private final long rootID;

    SubflowBuilder(ExecutorService cpuExecutor, ExecutorService ioExecutor, FlowEventPublisher flowEventPublisher, long rootID) {
        this.cpuExecutor = cpuExecutor;
        this.ioExecutor = ioExecutor;
        this.flowEventPublisher = flowEventPublisher;
        this.rootID = rootID;
    }

    public SubflowBuilder subflow(Consumer<AbstractFlowBuilder<?>> flowBuilderConsumer) {
        var builder = new FlowBuilder(cpuExecutor, ioExecutor,flowEventPublisher,rootID);
        flowBuilderConsumer.accept(builder);

        subflows.add(builder);

        return this;
    }

}
