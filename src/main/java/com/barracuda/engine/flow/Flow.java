package com.barracuda.engine.flow;

public interface Flow {

    void execute();

    FlowState state();

    long id();
}
