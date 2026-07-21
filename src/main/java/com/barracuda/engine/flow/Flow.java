package com.barracuda.engine.flow;

import com.barracuda.engine.event.ExecutionEvent;

public interface Flow {

    ScopedValue<FlowContext> FLOW_CONTEXT = ScopedValue.newInstance();

    void event(ExecutionEvent event);

    FlowState state();

    long id();
}
