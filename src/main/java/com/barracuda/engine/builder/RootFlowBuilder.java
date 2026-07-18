package com.barracuda.engine.builder;

import com.barracuda.engine.chain.ChainNode;
import com.barracuda.engine.flow.Flow;
import com.barracuda.engine.flow.FlowImpl;

import java.util.concurrent.ExecutorService;
import java.util.function.Function;

public class RootFlowBuilder extends FlowBuilder<RootFlowBuilder> {

    public RootFlowBuilder(ExecutorService cpuExecutor,ExecutorService ioExecutor) {
        super(cpuExecutor, ioExecutor);
    }

    public Flow build() {
        ChainNode current = null;

        for (Function<ChainNode, ChainNode> node : chainNodes.reversed()) {
            current = node.apply(current);
        }

        return new FlowImpl(current);
    }

    @Override
    protected RootFlowBuilder self() {
        return this;
    }
}
