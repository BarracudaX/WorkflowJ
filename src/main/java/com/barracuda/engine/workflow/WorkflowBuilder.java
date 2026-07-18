package com.barracuda.engine.workflow;

import com.barracuda.engine.flow.FlowImpl;
import com.barracuda.engine.step.Step;

import java.util.Objects;

public class WorkflowBuilder {

    private String name;
    private Step firstStep;
    private Step currentStep;

    public WorkflowBuilder name(String name) {
        this.name = name;
        return this;
    }

    public WorkflowBuilder step(Step step) {
        if(firstStep == null) {
            firstStep = step;
            currentStep = step;
        }
        currentStep.setNext(step);
        currentStep = step;
        return this;
    }

    public Workflow build() {
        return new Workflow(Objects.requireNonNull(name),new FlowImpl(firstStep));
    }


}
