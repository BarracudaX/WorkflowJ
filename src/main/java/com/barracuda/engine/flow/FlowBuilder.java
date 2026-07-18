package com.barracuda.engine.flow;

import com.barracuda.engine.step.Step;
import com.barracuda.engine.workflow.WorkflowBuilder;

public abstract class FlowBuilder<T extends FlowBuilder<T>> {

    protected Step firstStep;
    protected Step currentStep;

    public T step(Step step) {
        if(step == null) {
            throw new IllegalArgumentException("step cannot be null");
        }

        if(firstStep == null) {
            firstStep = step;
            currentStep = step;
        }
        currentStep.setNext(step);
        currentStep = step;
        return self();
    }

    protected abstract T self();
}
