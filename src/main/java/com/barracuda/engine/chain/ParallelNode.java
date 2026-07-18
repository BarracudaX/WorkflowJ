package com.barracuda.engine.chain;

import com.barracuda.engine.flow.Flow;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

public class ParallelNode implements ChainNode {

    private final List<Flow> subflows;
    private final ExecutorService executor;
    private final ChainNode next;

    public ParallelNode(List<Flow> subflows, ExecutorService executor,ChainNode next) {
        this.subflows = List.copyOf(subflows);
        this.executor = executor;
        this.next = next;
    }

    @Override
    public void execute() {

        var callables = subflows.stream().map(flow -> (Callable<Void>) () -> {
            flow.execute();
            return null;
        } ).toList();

        try {
            executor.invokeAll(callables);
            if (next != null) {
                next.execute();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
