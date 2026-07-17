package com.barracuda.engine.listener;

import com.barracuda.engine.event.WorkflowEvent;

public interface WorkflowEventListener {

    void event(WorkflowEvent event);

}
