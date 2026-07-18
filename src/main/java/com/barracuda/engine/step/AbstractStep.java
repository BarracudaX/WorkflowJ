package com.barracuda.engine.step;

public class AbstractStep implements Step {

    protected Step step;

    @Override
    public Step nextStep() {
        return step;
    }

    @Override
    public void setNext(Step step) {
        this.step = step;
    }
}
