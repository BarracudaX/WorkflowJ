package com.barracuda.engine.flow;

import com.barracuda.engine.chain.ChainNode;

import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.atomic.AtomicReference;

public class FlowImpl implements Flow {

    private final ChainNode chainNode;
    private final AtomicReference<FlowState> state = new AtomicReference<>(FlowState.CREATED);

    public FlowImpl(ChainNode chainNode) {
        this.chainNode = chainNode;
    }

    @Override
    public void execute() {
        if (!state.compareAndSet(FlowState.CREATED, FlowState.RUNNING) && !state.compareAndSet(FlowState.PAUSED, FlowState.RUNNING)) {
            throw new IllegalStateException("Flow cannot be executed because it is in invalid state. Flow state: "+ state.get());
        }

        try (var scope = StructuredTaskScope.open()){
            scope.fork(chainNode::execute);

            scope.join();
        } catch (Exception e) {
            handle(e);
        }

        state.set(FlowState.COMPLETED);
    }

    private void handle(Throwable exception){
        switch (exception) {
            case FlowInterruptedException ex -> interrupted(ex);
            case InterruptedException ex -> interrupted(new FlowInterruptedException("Flow Interrupted",ex));
            case StructuredTaskScope.FailedException ex -> handle(ex.getCause());
            case RuntimeException ex -> failed(ex);
            default -> failed(new RuntimeException(exception));
        }
    }

    private void interrupted(FlowInterruptedException ex){
        Thread.currentThread().interrupt();
        assert state.get() == FlowState.RUNNING;
        state.compareAndSet(FlowState.RUNNING, FlowState.PAUSED);
        throw ex;
    }

    private void failed(RuntimeException ex) {
        assert state.get() == FlowState.RUNNING;
        state.compareAndSet(FlowState.RUNNING, FlowState.FAILED);

        throw ex;
    }

    @Override
    public FlowState state() {
        return state.get();
    }
}
