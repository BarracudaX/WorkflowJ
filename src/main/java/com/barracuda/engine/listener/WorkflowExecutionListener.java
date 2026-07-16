package com.barracuda.engine.listener;

import com.barracuda.engine.event.WorkflowEvent;

public interface WorkflowExecutionListener {

    void event(WorkflowEvent event);

}
