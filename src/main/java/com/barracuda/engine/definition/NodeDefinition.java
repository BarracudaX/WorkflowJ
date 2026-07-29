package com.barracuda.engine.definition;

public interface NodeDefinition {

    long nodeID();

    void command(DefinitionCommand command);

    void event(DefinitionEvent event);
}
