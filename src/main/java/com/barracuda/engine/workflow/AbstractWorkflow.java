package com.barracuda.engine.workflow;

import com.barracuda.engine.work.Work;
import lombok.ToString;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Joiner;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Note workflow pausing should be done through the pause() method. In addition, of responsiveness is needed, users can interrupt the virtual thread in which the workflow is running but this should be done after calling the pause method.
 */
@ToString(onlyExplicitlyIncluded = true)
public abstract class AbstractWorkflow implements Workflow {

    public static final ScopedValue<WorkflowContext> WORKFLOW_CONTEXT = ScopedValue.newInstance();

    @ToString.Include
    protected final AtomicInteger currentlyRunningTaskIndex = new AtomicInteger(0);
    @ToString.Include
    private final String name;
    @ToString.Include
    private final long id;

    public AbstractWorkflow(String name, long id) {
        this.name = Objects.requireNonNull(name);
        this.id = id;
    }


    @Override
    public void execute() {
        if (!Thread.currentThread().isVirtual()) {
            throw new IllegalStateException("Workflow should be executed in a virtual thread.");
        }

        try {
            ScopedValue.where(WORKFLOW_CONTEXT, Objects.requireNonNull(context())).run(this::executeWorkflow);
        } catch (Exception ex) {
            workflowFailed(ex);
            throw ex;
        }
    }

    protected abstract void workflowFailed(Exception exception);

    private void executeWorkflow() {

        workflowStarting();

        var works = works();

        for (int i = currentlyRunningTaskIndex.get(); i < works.size(); i++) {

            Work work = works.get(i);

            try(var scope = StructuredTaskScope.open(Joiner.anySuccessfulOrThrow())) {

                scope.fork(work::execute);

                try {
                    scope.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    workflowInterrupted();
                    return;
                }
                workCompleted(work);
            }
        }

        workflowCompleted();
    }

    protected abstract void workflowStarting();

    protected abstract void workflowCompleted();

    protected abstract void workFailed(Exception e,Work work);

    protected abstract void workflowInterrupted();

    protected abstract List<Work> works();

    private void workCompleted(Work work) {
        currentlyRunningTaskIndex.incrementAndGet();
    }

    protected abstract WorkflowContext context();

}
