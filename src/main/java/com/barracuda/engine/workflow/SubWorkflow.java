package com.barracuda.engine.workflow;

import com.barracuda.engine.event.WorkflowEvent.*;
import com.barracuda.engine.work.Work;

import java.util.List;

public class SubWorkflow extends AbstractWorkflow {

    private final List<Work> works;

    public SubWorkflow(String name, long id, List<Work> works) {
        super(name, id);
        this.works = List.copyOf(works);
    }


    @Override
    protected void workflowFailed(Exception exception) {
//        WORKFLOW_CONTEXT.get().getEventPublisher().publishEvent(new SubWorkflowFailedEvent());
    }

    @Override
    protected void workflowStarting() {
        if(currentlyRunningTaskIndex.get() == 0){
//            WORKFLOW_CONTEXT.get().getEventPublisher().publishEvent(new SubWorkflowStartedEvent());
        }else {
//            WORKFLOW_CONTEXT.get().getEventPublisher().publishEvent(new SubWorkflowResumedEvent());
        }
    }

    @Override
    protected void workflowCompleted() {
//        WORKFLOW_CONTEXT.get().getEventPublisher().publishEvent(new SubWorkflowCompletedEvent());
    }

    @Override
    protected void workFailed(Exception e, Work work) {
//        WORKFLOW_CONTEXT.get().getEventPublisher().publishEvent(new SubWorkflowFailedEvent());
    }

    @Override
    protected void workflowInterrupted() {
//        WORKFLOW_CONTEXT.get().getEventPublisher().publishEvent(new SubWorkflowPausedEvent());
    }

    @Override
    protected List<Work> works() {
        return works;
    }

    @Override
    protected WorkflowContext context() {
        return new WorkflowContext(WORKFLOW_CONTEXT.get());
    }

}
