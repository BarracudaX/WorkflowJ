package com.barracuda.engine.flow;

public interface Flow {

    ScopedValue<FlowContext> FLOW_CONTEXT = ScopedValue.newInstance();

    void execute();

    FlowState state();

    long id();
}
