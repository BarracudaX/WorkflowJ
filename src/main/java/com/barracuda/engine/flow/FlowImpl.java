package com.barracuda.engine.flow;

import com.barracuda.engine.step.Step;

import java.util.Objects;

public class FlowImpl implements Flow {

    private final Step step;

    public FlowImpl(Step step) {
        this.step = Objects.requireNonNull(step);
    }


    @Override
    public Step step() {
        return step;
    }

}
