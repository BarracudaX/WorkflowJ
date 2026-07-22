package com.barracuda.engine.flow;

import com.barracuda.engine.event.FlowEventPublisher;

public record FlowContext(FlowEventPublisher flowEventPublisher, long rootID) {

}
