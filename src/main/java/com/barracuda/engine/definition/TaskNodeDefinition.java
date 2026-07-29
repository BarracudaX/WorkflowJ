package com.barracuda.engine.definition;


import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class TaskNodeDefinition implements NodeDefinition{

    @EqualsAndHashCode.Include
    private final long nodeID;

    public TaskNodeDefinition(long nodeID) {
        this.nodeID = nodeID;
    }

    @Override
    public long nodeID() {
        return nodeID;
    }

    @Override
    public void command(DefinitionCommand command) {

    }

    @Override
    public void event(DefinitionEvent event) {

    }
}
