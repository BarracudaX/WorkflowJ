package com.barracuda.engine.workflow;


import com.barracuda.engine.flow.Flow;

import java.util.Objects;

public class Workflow {

    private final String name;
    private final Flow flow;

    public Workflow(String name, Flow flow) {
        this.name = Objects.requireNonNull(name);
        this.flow = flow;
    }

    public static WorkflowBuilder workflow() {
        return new WorkflowBuilder();
    }

    public String name() {
        return name;
    }

    public Flow flow() { return flow; }
}
