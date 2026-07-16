package com.barracuda.engine.workflow;

import com.barracuda.engine.domain.WorkflowStatus;
import com.barracuda.engine.event.WorkflowEventPublisher;
import com.barracuda.engine.listener.WorkflowExecutionListener;

public interface RootWorkflow extends Workflow, WorkflowEventPublisher {

    /**
     * Registered this listener for workflow events
     * @param listener to add to the listeners of this workflow
     */
    void registerListener(WorkflowExecutionListener listener);

    WorkflowStatus status();

}
