package com.barracuda.engine.definition;

public sealed interface DefinitionEvent {

    sealed interface FlowDefinitionEvent extends DefinitionEvent{

        long targetFlowDefinition();

        long taskNodeID();

        record TaskNodeAddedEvent(long targetFlowDefinition, long taskNodeID) implements FlowDefinitionEvent { }

        record TaskNodeRemovedEvent(long targetFlowDefinition, long taskNodeID) implements FlowDefinitionEvent { }

        record ParallelNodeAddedEvent(long targetFlowDefinition, long taskNodeID) implements FlowDefinitionEvent { }

        record ParallelNodeRemovedEvent(long targetFlowDefinition, long taskNodeID) implements FlowDefinitionEvent { }
    }

    sealed interface ParallelNodeDefinitionEvent extends DefinitionEvent{

        long targetParallelNodeDefinition();

        record SubflowAddedEvent(long targetParallelNodeDefinition, long subflowID) implements ParallelNodeDefinitionEvent {}
    }

}
