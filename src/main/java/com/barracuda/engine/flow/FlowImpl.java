package com.barracuda.engine.flow;

import com.barracuda.engine.chain.ChainNode;

import java.util.concurrent.atomic.AtomicReference;

public class FlowImpl implements Flow {

    private final ChainNode chainNode;
    private final AtomicReference<FlowState> state = new AtomicReference<>(FlowState.CREATED);

    public FlowImpl(ChainNode chainNode) {
        this.chainNode = chainNode;
    }

    @Override
    public void execute() {
        if (!state.compareAndSet(FlowState.CREATED, FlowState.RUNNING)) {
            throw new IllegalStateException("Flow cannot be executed because it is in invalid state. Flow state: "+ state.get());
        }

        try {
            chainNode.execute();
        } catch (Exception e) {
            assert state.get() == FlowState.RUNNING;
            state.compareAndSet(FlowState.RUNNING, FlowState.FAILED);
            throw e;
        }

        state.set(FlowState.COMPLETED);
    }

    @Override
    public FlowState state() {
        return state.get();
    }
}
