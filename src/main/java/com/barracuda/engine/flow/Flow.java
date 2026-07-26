package com.barracuda.engine.flow;

import com.barracuda.engine.chain.ChainNode;

public interface Flow extends ChainNode {

    ScopedValue<FlowContext> FLOW_CONTEXT = ScopedValue.newInstance();

    FlowStatus state();

    long id();
}
