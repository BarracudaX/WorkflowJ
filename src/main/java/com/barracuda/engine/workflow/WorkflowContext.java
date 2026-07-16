package com.barracuda.engine.workflow;

import com.barracuda.engine.event.WorkflowEventPublisher;
import com.barracuda.engine.store.WorkflowStore;
import lombok.Getter;

import java.time.Duration;
import java.util.concurrent.ExecutorService;

public final class WorkflowContext {
    @Getter
    private final ExecutorService cpuExecutorService;
    @Getter
    private final Duration cpuTimeSlot;
    @Getter
    private final WorkflowEventPublisher eventPublisher;
    @Getter
    private final WorkflowStore workflowStore;
    private final WorkflowContext parent;



    public WorkflowContext(ExecutorService cpuExecutorService, Duration cpuTimeSlot, WorkflowEventPublisher eventPublisher, WorkflowStore workflowStore) {
        this.cpuExecutorService = cpuExecutorService;
        this.cpuTimeSlot = cpuTimeSlot;
        this.eventPublisher = eventPublisher;
        this.workflowStore = workflowStore;
        this.parent = null;
    }

    public WorkflowContext(WorkflowContext parent) {
        this.parent = parent;
        this.workflowStore = parent.getWorkflowStore();
        this.cpuExecutorService = parent.getCpuExecutorService();
        this.cpuTimeSlot = parent.getCpuTimeSlot();
        this.eventPublisher = parent.getEventPublisher();
    }

}
