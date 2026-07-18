package com.barracuda.engine.flow;

import com.barracuda.engine.step.Step;

public class FlowImpl implements Flow {

    private final Step step;

    public FlowImpl(Step step) {
        this.step = step;
    }

    @Override
    public Step firstStep() {
        return step;
    }

    @Override
    public Step nextStep() {
        return step.nextStep();
    }

}
