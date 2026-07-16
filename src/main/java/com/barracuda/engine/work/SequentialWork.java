package com.barracuda.engine.work;

import com.barracuda.engine.domain.TaskResult;
import com.barracuda.engine.domain.TimeSlot;
import com.barracuda.engine.task.CpuTask;
import com.barracuda.engine.task.IOTask;
import com.barracuda.engine.task.Task;
import com.barracuda.engine.test.TaskNeedMoreTimeException;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Joiner;
import java.util.concurrent.atomic.AtomicInteger;

import static com.barracuda.engine.workflow.AbstractWorkflow.WORKFLOW_CONTEXT;

/**
 * An abstraction of unit of work that consists of steps.
 */
public class SequentialWork extends AbstractWork {

    private final AtomicInteger currentlyRunningTaskIndex = new AtomicInteger(0);
    private final List<Task> tasks;

    public SequentialWork(String name, long id, List<Task> tasks) {
        super(name, id);
        this.tasks = List.copyOf(tasks);
    }

    @Override
    protected final void executeWork() {
        int startIndex = currentlyRunningTaskIndex.get();

        for (int i = startIndex; i < tasks.size(); i++) {

            try(var scope = StructuredTaskScope.open(Joiner.<TaskResult>anySuccessfulOrThrow())){
                Task task = tasks.get(i);

                scope.fork(() -> runTask(task));

                try {
                    scope.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

    }

    @Override
    protected void workFailed(Exception ex) {

    }


    private void runTask(Task task) {
        switch (task){
            case CpuTask cpuTask -> runCPUTask(cpuTask);
            case IOTask ioTask -> runIOTask(ioTask);
        }
    }

    private void runCPUTask(CpuTask cpuTask) {

        ExecutorService cpuExecutorService = WORKFLOW_CONTEXT.get().getCpuExecutorService();
        Duration timeSlot = WORKFLOW_CONTEXT.get().getCpuTimeSlot();

        while(!Thread.currentThread().isInterrupted()) {

            Future<?> cpuTaskFuture = cpuExecutorService.submit(() -> cpuTask.execute(new TimeSlot(timeSlot)));

            try {
                cpuTaskFuture.get();
                return;
            } catch (InterruptedException e) {
                cpuTaskFuture.cancel(true);
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof TaskNeedMoreTimeException)) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void runIOTask(IOTask ioTask) {
        ioTask.execute();
    }

}
