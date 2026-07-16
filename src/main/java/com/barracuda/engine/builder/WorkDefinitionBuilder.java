package com.barracuda.engine.builder;

import com.barracuda.engine.definition.WorkDefinition;

import java.util.Objects;

public class WorkDefinitionBuilder {

    private String name;
    private String description;


    public WorkDefinitionBuilder name(String name) {
        Objects.requireNonNull(name,"Step definition name cannot be null");

        if(name.isBlank()){
            throw new IllegalArgumentException("Step definition name cannot be blank");
        }

        this.name = name;
        return this;
    }

    public WorkDefinitionBuilder description(String description) {
        Objects.requireNonNull(description,"Step definition description cannot be null");

        if(description.isBlank()){
            throw new IllegalArgumentException("Step definition description cannot be blank");
        }

        this.description = description;

        return this;
    }

    WorkDefinition build(){
        return new WorkDefinition(name,description);
    }

}
