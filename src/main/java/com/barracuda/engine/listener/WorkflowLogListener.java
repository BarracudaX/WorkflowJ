package com.barracuda.engine.listener;

import com.barracuda.engine.event.WorkflowEvent;
import com.barracuda.engine.event.WorkflowEvent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkflowLogListener implements WorkflowExecutionListener {

    private final Logger logger = LoggerFactory.getLogger(WorkflowLogListener.class);

    @Override
    public void event(WorkflowEvent event) {
        switch (event) {
            case SubWorkflowCompletedEvent subWorkflowCompletedEvent -> {
            }
            case SubWorkflowFailedEvent subWorkflowFailedEvent -> {
            }
            case SubWorkflowPausedEvent subWorkflowPausedEvent -> {
            }
            case SubWorkflowResumedEvent subWorkflowResumedEvent -> {
            }
            case SubWorkflowStartedEvent subWorkflowStartedEvent -> {
            }
            case TaskCompletedEvent taskCompletedEvent -> {
            }
            case TaskFailedEvent taskFailedEvent -> {
            }
            case TaskPausedEvent taskPausedEvent -> {
            }
            case TaskResumedEvent taskResumedEvent -> {
            }
            case TaskStartedEvent taskStartedEvent -> {
            }
            case WorkCompletedEvent workCompletedEvent -> {
            }
            case WorkFailedEvent workFailedEvent -> {
            }
            case WorkPausedEvent workPausedEvent -> {
            }
            case WorkResumedEvent workResumedEvent -> {
            }
            case WorkStartedEvent workStartedEvent -> {
            }
            case WorkflowCompletedEvent workflowCompletedEvent -> {
            }
            case WorkflowFailedEvent workflowFailedEvent -> {
            }
            case WorkflowPausedEvent workflowPausedEvent -> {
            }
            case WorkflowResumedEvent workflowResumedEvent -> {
            }
            case WorkflowStartedEvent workflowStartedEvent -> {
            }
        }
    }

}
