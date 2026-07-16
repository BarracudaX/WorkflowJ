package com.barracuda.engine.builder;

import com.barracuda.engine.definition.WorkflowDefinition;

/**
 * Used by clients to build workflows.
 */
public class RootWorkflowDefinitionBuilder extends AbstractWorkflowDefinitionBuilder<RootWorkflowDefinitionBuilder>{


    public WorkflowDefinition build(){
        return new WorkflowDefinition(name, description, workDefinitions);
    }

    public static RootWorkflowDefinitionBuilder builder() {
        return new RootWorkflowDefinitionBuilder();
    }

    @Override
    public RootWorkflowDefinitionBuilder self() {
        return this;
    }
}
