package com.barracuda.engine.workflow;

import com.barracuda.engine.flow.FlowBuilder;
import com.barracuda.engine.flow.RootFlowBuilder;

import java.util.function.Consumer;

public class WorkflowBuilder {

    private String name;
    private final RootFlowBuilder flowBuilder = new RootFlowBuilder();

    public WorkflowBuilder name(String name) {
        if (name == null) {
            throw new IllegalArgumentException("name cannot be null");
        }

        if (name.isBlank()) {
            throw new IllegalArgumentException("name cannot be blank");
        }
        this.name = name;
        return this;
    }

    public WorkflowBuilder flow(Consumer<FlowBuilder<?>> flowBuilderConsumer) {
        flowBuilderConsumer.accept(flowBuilder);

        return this;
    }

    public Workflow build() {
        return new Workflow(name,flowBuilder.build());
    }


}
