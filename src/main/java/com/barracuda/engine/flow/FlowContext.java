package com.barracuda.engine.flow;

import com.barracuda.engine.event.FlowEventPublisher;
import lombok.Getter;

public class FlowContext {

    @Getter
    private final FlowEventPublisher flowEventPublisher;

    public FlowContext(FlowEventPublisher flowEventPublisher) {
        this.flowEventPublisher = flowEventPublisher;
    }
}
