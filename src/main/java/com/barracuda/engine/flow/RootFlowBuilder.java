package com.barracuda.engine.flow;

public class RootFlowBuilder extends FlowBuilder<RootFlowBuilder> {

    public Flow build() {
        return new FlowImpl(firstStep);
    }

    @Override
    protected RootFlowBuilder self() {
        return this;
    }
}
