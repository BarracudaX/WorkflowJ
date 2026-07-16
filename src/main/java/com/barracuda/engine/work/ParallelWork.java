package com.barracuda.engine.work;

import com.barracuda.engine.workflow.SubWorkflow;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.StructuredTaskScope.Joiner;

public class ParallelWork extends AbstractWork {

    private final List<SubWorkflow> workflows;
    private final Work nextWork;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public ParallelWork(String name, long id, List<SubWorkflow> workflows, Work nextWork) {
        super(name,id);
        this.workflows = List.copyOf(workflows);
        this.nextWork = nextWork;
    }

    @Override
    protected void executeWork() {
        try(var scope = StructuredTaskScope.open(Joiner.awaitAllSuccessfulOrThrow())){

            for(var subworkflow : workflows){
                scope.fork(subworkflow::execute);
            }

            try {
                scope.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        if(nextWork != null) {
            nextWork.execute();
        }
    }

    @Override
    protected void workFailed(Exception ex) {
    }

}
