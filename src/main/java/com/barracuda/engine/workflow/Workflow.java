package com.barracuda.engine.workflow;

import com.barracuda.engine.domain.WorkflowStatus;

public interface Workflow {

    /**
     * Runs or resumes the workflow.
     */
    void execute();

    WorkflowStatus status();

    long id();
}
