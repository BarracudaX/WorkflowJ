package com.barracuda.engine.event;


/**
 * An event publisher associated with a workflow that allows publishing workflow events for the associated workflow.
 */
public interface WorkflowEventPublisher {

    void publishEvent(WorkflowEvent event);

}
