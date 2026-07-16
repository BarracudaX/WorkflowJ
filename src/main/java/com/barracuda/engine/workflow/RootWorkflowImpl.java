package com.barracuda.engine.workflow;

import com.barracuda.engine.domain.WorkflowStatus;
import com.barracuda.engine.event.WorkflowEvent;
import com.barracuda.engine.event.WorkflowEvent.WorkflowFailedEvent;
import com.barracuda.engine.event.WorkflowEvent.WorkflowResumedEvent;
import com.barracuda.engine.event.WorkflowEvent.WorkflowStartedEvent;
import com.barracuda.engine.listener.WorkflowExecutionListener;
import com.barracuda.engine.store.WorkflowStore;
import com.barracuda.engine.work.Work;
import lombok.ToString;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;


public class RootWorkflowImpl extends AbstractWorkflow implements RootWorkflow{


    private final CopyOnWriteArrayList<WorkflowExecutionListener> listeners = new CopyOnWriteArrayList<>();
    private final WorkflowContext context;
    @ToString.Include
    private final AtomicReference<WorkflowStatus> workflowStatus = new AtomicReference<>(WorkflowStatus.INITIALIZED);
    private final List<Work> works = Collections.synchronizedList(new ArrayList<>());

    public RootWorkflowImpl(String name, long id, Duration cpuTimeSlot, ExecutorService cpuExecutorService, WorkflowStore workflowStore, List<Work> works) {
        super(name, id);
        this.works.addAll(works);
        this.context = new WorkflowContext(cpuExecutorService, cpuTimeSlot, this,workflowStore);
    }

    @Override
    protected void workflowFailed(Exception exception) {
        if(!workflowStatus.compareAndSet(WorkflowStatus.RUNNING, WorkflowStatus.FAILED)){
            throw new IllegalStateException("Workflow failed with an exception while not running. Curren status: "+workflowStatus.get(),exception);
        }
//        publishEvent(new WorkflowFailedEvent());
    }

    @Override
    protected void workflowStarting() {
        if(!workflowStatus.compareAndSet(WorkflowStatus.INITIALIZED, WorkflowStatus.RUNNING) && !workflowStatus.compareAndSet(WorkflowStatus.PAUSED, WorkflowStatus.RUNNING)) {
            throw new IllegalStateException("Workflow cannot be executed due to its current state being "+ workflowStatus.get().name()+". The workflow needs to be in either INITIALIZED or PAUSED state in order to be executed.");
        }
        publishStartEvent();
    }

    @Override
    protected void workflowCompleted() {
//        publishEvent(new WorkflowEvent.WorkflowCompletedEvent());

        workflowStatus.set(WorkflowStatus.COMPLETED);
    }

    @Override
    protected void workFailed(Exception e, Work work) {
//        publishEvent(new WorkflowFailedEvent());

        workflowStatus.set(WorkflowStatus.FAILED);
    }

    @Override
    protected void workflowInterrupted() {
        if (!workflowStatus.compareAndSet(WorkflowStatus.RUNNING, WorkflowStatus.PAUSED)) {
            throw new IllegalStateException("Requested pausing while workflow isn't running.");
        }

//        publishEvent(new WorkflowEvent.WorkflowPausedEvent());
    }

    private void publishStartEvent() {
        if (currentlyRunningTaskIndex.get() == 0) {
//            publishEvent(new WorkflowStartedEvent());
        } else {
//            publishEvent(new WorkflowResumedEvent());
        }
    }

    @Override
    protected List<Work> works() {
        return works;
    }

    @Override
    protected WorkflowContext context() {
        return context;
    }

    @Override
    public void registerListener(WorkflowExecutionListener listener){
        listeners.add(listener);
    }

    @Override
    public WorkflowStatus status() {
        return workflowStatus.get();
    }


    public void publishEvent(WorkflowEvent event){
        for(WorkflowExecutionListener listener : listeners){
            listener.event(event);
        }
    }
}
