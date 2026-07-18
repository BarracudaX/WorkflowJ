package com.barracuda.engine.step;

public interface Step {

    Step nextStep();

    void setNext(Step step);
}
