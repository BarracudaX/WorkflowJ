package com.barracuda.engine.builder;

import com.barracuda.engine.definition.WorkDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public abstract class AbstractWorkflowDefinitionBuilder<T extends AbstractWorkflowDefinitionBuilder<T>> {


    private final WorkflowDefinitionConfigurer<T> configurer = new WorkflowDefinitionConfigurer<>(this);

    protected final List<WorkDefinition> workDefinitions = new ArrayList<>();
    protected String name;
    protected String description;

    public T workflowConfig(Consumer<WorkflowDefinitionConfigurer<T>> consumer) {
        consumer.accept(configurer);
        return self();
    }

    public T work(Consumer<WorkDefinitionBuilder> stepConfigurer){
        WorkDefinitionBuilder workDefinitionBuilder = new WorkDefinitionBuilder();

        stepConfigurer.accept(workDefinitionBuilder);

        workDefinitions.add(workDefinitionBuilder.build());

        return self();
    }

    public T decisionStep(Consumer<DecisionControlDefinitionBuilder> stepConfigurer){
        DecisionControlDefinitionBuilder decisionControlDefinitionBuilder = new DecisionControlDefinitionBuilder();

        stepConfigurer.accept(decisionControlDefinitionBuilder);

        workDefinitions.add(decisionControlDefinitionBuilder.build());

        return self();
    }

    public T parallelStep(Consumer<ParallelControlDefinitionBuilder> stepConfigurer){
        ParallelControlDefinitionBuilder parallelControlDefinitionBuilder = new ParallelControlDefinitionBuilder();

        stepConfigurer.accept(parallelControlDefinitionBuilder);

        workDefinitions.add(parallelControlDefinitionBuilder.build());

        return self();
    }

    public abstract T self();



    public static class WorkflowDefinitionConfigurer<T extends AbstractWorkflowDefinitionBuilder<T>>{

        private final AbstractWorkflowDefinitionBuilder<T> builder;

        private WorkflowDefinitionConfigurer(AbstractWorkflowDefinitionBuilder<T> builder){
            this.builder = builder;
        }

        public WorkflowDefinitionConfigurer<T> name(String name) {
            Objects.requireNonNull(name, "Workflow name cannot be null");

            if(name.isBlank()){
                throw new IllegalArgumentException("Workflow name cannot be blank");
            }

            builder.name = name;
            return this;
        }

        public WorkflowDefinitionConfigurer<T> description(String description) {
            Objects.requireNonNull(description, "Workflow description cannot be null");

            if(description.isBlank()){
                throw new IllegalArgumentException("Workflow description cannot be blank");
            }

            builder.description = description;

            return this;
        }

    }



}
