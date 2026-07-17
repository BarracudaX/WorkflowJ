package com.barracuda.engine.workflow;

import com.barracuda.engine.listener.WorkflowEventListener;

public interface RootWorkflow extends Workflow {

    /**
     * Registered this listener for workflow events
     * @param listener to add to the listeners of this workflow
     */
    void registerListener(WorkflowEventListener listener);

}
