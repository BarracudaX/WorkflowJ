package com.barracuda.engine.test.builder;

import com.barracuda.engine.test.flow.TestFlow;

public class TestFlowBuilder extends AbstractTestFlowBuilder<TestFlowBuilder> {

    public static TestFlowBuilder testFlow() {
        return new TestFlowBuilder();
    }

    public TestFlow build() {
        return new TestFlow(flowBuilder.build(), testTasks ,eventCapturer, subflowsMap);
    }

    @Override
    protected TestFlowBuilder self() {
        return this;
    }
}
