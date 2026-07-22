package com.barracuda.engine.builder;

import com.barracuda.engine.chain.ChainNode;
import com.barracuda.engine.event.FlowEventPublisher;
import com.barracuda.engine.event.NoOpEvenPublisher;
import com.barracuda.engine.flow.Flow;
import com.barracuda.engine.flow.FlowContext;
import com.barracuda.engine.flow.FlowImpl;

import java.util.concurrent.ExecutorService;
import java.util.function.Function;

public class FlowBuilder extends AbstractFlowBuilder<FlowBuilder> {


    public FlowBuilder(long flowID, ExecutorService cpuExecutor, ExecutorService ioExecutor, FlowEventPublisher flowEventPublisher) {
        super(flowID, cpuExecutor, ioExecutor,flowEventPublisher, flowID);
    }

    public FlowBuilder(long flowID, ExecutorService cpuExecutor, ExecutorService ioExecutor) {
        super(flowID, cpuExecutor, ioExecutor, new NoOpEvenPublisher(), flowID);
    }

    FlowBuilder(long subflowID, ExecutorService cpuExecutor, ExecutorService ioExecutor, FlowEventPublisher flowEventPublisher, long rootID) {
        super(subflowID, cpuExecutor, ioExecutor, flowEventPublisher, rootID);
    }

    public Flow build() {
        ChainNode current = null;

        for (Function<ChainNode, ChainNode> node : chainNodes.reversed()) {
            current = node.apply(current);
        }

        var context = new FlowContext(flowEventPublisher, flowID);

        return new FlowImpl(current, context,flowID);
    }

    @Override
    protected FlowBuilder self() {
        return this;
    }
}
