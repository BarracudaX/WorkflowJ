package com.barracuda.engine.definition;

import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@ToString
public class ParallelWorkDefinition extends WorkDefinition {

    private final List<WorkflowDefinition> workflows = new ArrayList<>();

    public ParallelWorkDefinition(String name, String description, List<WorkflowDefinition> workflows) {
        super(name, description);
        this.workflows.addAll(workflows);
    }



}
