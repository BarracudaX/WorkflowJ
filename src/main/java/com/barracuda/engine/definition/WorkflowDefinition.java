package com.barracuda.engine.definition;

import java.util.List;

public record WorkflowDefinition(String name, String description, List<WorkDefinition> steps) {

    public WorkflowDefinition{
        steps = List.copyOf(steps);
    }

}
