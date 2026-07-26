package com.barracuda.engine.flow;

import com.barracuda.engine.chain.ChainNode;
import com.barracuda.engine.event.ExecutionEvent;

public interface Flow extends ChainNode {

    ScopedValue<FlowContext> FLOW_CONTEXT = ScopedValue.newInstance();

    FlowState state();

    long id();
}
