package com.barracuda.engine.chain;

import com.barracuda.engine.flow.Flow;
import com.barracuda.engine.flow.FlowInterruptedException;

import java.util.List;
import java.util.concurrent.*;

public class ParallelNode implements ChainNode {

    private final List<Flow> subflows;
    private final ChainNode next;

    public ParallelNode(List<Flow> subflows, ChainNode next) {
        this.subflows = List.copyOf(subflows);
        this.next = next;
    }

    @Override
    public void execute() {

        try (var scope = StructuredTaskScope.open()) {
            subflows.forEach(subflow -> scope.fork(subflow::execute));
            scope.join();
        } catch (Exception exception) {
            handle(exception);
        }

        if (next != null) {
            next.execute();
        }
    }

    private void handle(Throwable exception) {
        switch (exception) {
            case InterruptedException ex ->{
                Thread.currentThread().interrupt();
                throw new FlowInterruptedException("Flow interrupted",ex);
            }
            case StructuredTaskScope.FailedException ex -> handle(ex.getCause());
            case RuntimeException ex -> throw ex;
            default -> throw new RuntimeException(exception);
        }
    }

}
