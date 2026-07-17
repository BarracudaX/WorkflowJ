package com.barracuda.engine.event;


import com.barracuda.engine.listener.WorkflowEventListener;

/**
 * An event publisher associated with a workflow that allows publishing workflow events for the associated workflow.
 */
public interface WorkflowEventPublisher {

    void fire(WorkflowEvent event);

    void registerListener(WorkflowEventListener listener);
}
