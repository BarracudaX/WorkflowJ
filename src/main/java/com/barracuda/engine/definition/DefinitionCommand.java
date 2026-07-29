package com.barracuda.engine.definition;

public sealed interface DefinitionCommand {

    sealed interface FlowDefinitionCommand extends DefinitionCommand{
        long targetFlowDefinition();

        long nodeID();

        record AddTaskNode(long targetFlowDefinition, long nodeID) implements FlowDefinitionCommand{
        }

        record RemoveTaskNode(long targetFlowDefinition, long nodeID) implements FlowDefinitionCommand{

        }

        record AddParallelNode(long targetFlowDefinition, long nodeID) implements FlowDefinitionCommand {
        }

        record RemoveParallelNode(long targetFlowDefinition, long nodeID) implements FlowDefinitionCommand {}
    }

    sealed interface ParallelNodeDefinitionCommand extends DefinitionCommand{

        long targetParallelNodeDefinition();

        record AddSubflow(long targetParallelNodeDefinition, long subflowID) implements ParallelNodeDefinitionCommand {}

        record RemoveSubflowCommand(long targetParallelNodeDefinition, long subflowID) implements ParallelNodeDefinitionCommand {}
    }

}
