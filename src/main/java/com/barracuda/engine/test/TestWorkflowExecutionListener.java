package com.barracuda.engine.test;

import com.barracuda.engine.workflow.AbstractWorkflow;
import com.barracuda.engine.event.WorkflowEvent;
import com.barracuda.engine.listener.WorkflowExecutionListener;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Listeners that records workflow events and provides API for verification.
 */
public class TestWorkflowExecutionListener implements WorkflowExecutionListener {

    private final ConcurrentHashMap<AbstractWorkflow, List<WorkflowEvent>> events = new ConcurrentHashMap<>();
    private final AtomicBoolean rootWorkflowCompleted = new AtomicBoolean(false);

    public void verify(){
        if(!rootWorkflowCompleted.get()){
            throw new IllegalStateException("The root workflow hasn't completed yet.");
        }
    }

    @Override
    public void event(WorkflowEvent event) {
        if(rootWorkflowCompleted.get()){
            throw new IllegalStateException("The root workflow has completed, yet new event has arrived "+event+" .");
        }
    }
}
