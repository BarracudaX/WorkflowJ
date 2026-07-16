package com.barracuda.engine.builder;

import com.barracuda.engine.definition.DecisionWorkDefinition;
import com.barracuda.engine.definition.WorkflowDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public class DecisionControlDefinitionBuilder {

    private String name;
    private String description;
    private final List<WorkflowDefinition> workflowDefinitions = new ArrayList<>();

    public DecisionControlDefinitionBuilder decision(Consumer<WorkflowDefinitionBuilder>  decisionConfigurer){
        WorkflowDefinitionBuilder workflowDefinitionBuilder = new WorkflowDefinitionBuilder();

        decisionConfigurer.accept(workflowDefinitionBuilder);

        workflowDefinitions.add(workflowDefinitionBuilder.build());

        return this;
    }

    public DecisionControlDefinitionBuilder name(String name) {
        Objects.requireNonNull(name,"Step definition name cannot be null");

        if(name.isBlank()){
            throw new IllegalArgumentException("Step definition name cannot be blank");
        }

        this.name = name;
        return this;
    }

    public DecisionControlDefinitionBuilder description(String description) {
        Objects.requireNonNull(description,"Step definition description cannot be null");

        if(description.isBlank()){
            throw new IllegalArgumentException("Step definition description cannot be blank");
        }

        this.description = description;

        return this;
    }

    DecisionWorkDefinition build() {
        return new DecisionWorkDefinition(name,description);
    }
}
