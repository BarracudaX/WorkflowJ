package com.barracuda.engine.flow;

public abstract class AbstractFlowBuilder<T extends AbstractFlowBuilder<T>> {

    protected abstract T self();

}
