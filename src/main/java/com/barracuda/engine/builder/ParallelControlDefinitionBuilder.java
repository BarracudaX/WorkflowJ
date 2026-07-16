package com.barracuda.engine.builder;

import com.barracuda.engine.definition.ParallelWorkDefinition;
import com.barracuda.engine.definition.WorkflowDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public class ParallelControlDefinitionBuilder {

    private String name;
    private String description;
    private final List<WorkflowDefinition> workflowDefinitions = new ArrayList<>();

    public ParallelControlDefinitionBuilder path(Consumer<WorkflowDefinitionBuilder> pathConfigurer){

        WorkflowDefinitionBuilder workflowDefinitionBuilder = new WorkflowDefinitionBuilder();

        pathConfigurer.accept(workflowDefinitionBuilder);

        workflowDefinitions.add(workflowDefinitionBuilder.build());

        return this;
    }

    public ParallelControlDefinitionBuilder name(String name) {
        Objects.requireNonNull(name,"Step definition name cannot be null");

        if(name.isBlank()){
            throw new IllegalArgumentException("Step definition name cannot be blank");
        }

        this.name = name;
        return this;
    }

    public ParallelControlDefinitionBuilder description(String description) {
        Objects.requireNonNull(description,"Step definition description cannot be null");

        if(description.isBlank()){
            throw new IllegalArgumentException("Step definition description cannot be blank");
        }

        this.description = description;

        return this;
    }


    ParallelWorkDefinition build(){
        return new ParallelWorkDefinition(name, description, workflowDefinitions);
    }
}
