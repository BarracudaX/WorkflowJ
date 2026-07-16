package com.barracuda.engine.event;

public sealed interface WorkflowEvent {

    record WorkflowStartedEvent(long workflowID) implements WorkflowEvent{ }

    record WorkflowCompletedEvent(long workflowID) implements WorkflowEvent{}

    record WorkflowFailedEvent(Exception ex, long workflowID) implements WorkflowEvent{}

    record WorkflowPausedEvent(long workflowID) implements WorkflowEvent{}

    record WorkflowResumedEvent(long workflowID) implements WorkflowEvent{}

    record WorkStartedEvent(long workflowID, long workID) implements WorkflowEvent{}

    record WorkCompletedEvent(long workflowID, long workID) implements WorkflowEvent{}

    record WorkFailedEvent(Exception ex, long workflowID, long workID) implements WorkflowEvent{}

    record WorkPausedEvent(long workflowID, long workID) implements WorkflowEvent{}

    record WorkResumedEvent(long workflowID, long workID) implements WorkflowEvent{}

    record TaskStartedEvent(long workflowID, long workID, long taskID) implements WorkflowEvent{}

    record TaskCompletedEvent(long workflowID, long workID, long taskID) implements WorkflowEvent{}

    record TaskFailedEvent(Exception ex, long workflowID, long workID, long taskID) implements WorkflowEvent{}

    record TaskPausedEvent(long workflowID, long workID, long taskID) implements WorkflowEvent{}

    record TaskResumedEvent(long workflowID, long workID, long taskID) implements WorkflowEvent{}

    record SubWorkflowStartedEvent(long workflowID, long subWorkflowID) implements WorkflowEvent{}

    record SubWorkflowCompletedEvent(long workflowID, long subWorkflowID) implements WorkflowEvent{}

    record SubWorkflowFailedEvent(Exception ex, long workflowID, long subWorkflowID) implements WorkflowEvent{}

    record SubWorkflowPausedEvent(long workflowID, long subWorkflowID) implements WorkflowEvent{}

    record SubWorkflowResumedEvent(long workflowID, long subWorkflowID) implements WorkflowEvent{}
}


