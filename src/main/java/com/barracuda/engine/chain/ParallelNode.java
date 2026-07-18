package com.barracuda.engine.chain;

import com.barracuda.engine.flow.Flow;

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
        try(var scope = StructuredTaskScope.open()) {
            subflows.forEach(subflow -> scope.fork(subflow::execute));

            scope.join();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (StructuredTaskScope.FailedException e) {
            handle(e);
        }

        if (next != null) {
            next.execute();
        }
    }

    private void handle(StructuredTaskScope.FailedException ex){
        if(ex.getCause() instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        throw new RuntimeException(ex);
    }
}
