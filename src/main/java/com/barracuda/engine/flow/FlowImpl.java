package com.barracuda.engine.flow;

import com.barracuda.engine.chain.ChainNode;

public class FlowImpl implements Flow {

    private final ChainNode chainNode;

    public FlowImpl(ChainNode chainNode) {
        this.chainNode = chainNode;
    }


    @Override
    public void execute() {
        chainNode.execute();
    }
}
