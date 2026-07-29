package com.barracuda.engine.definition;

import com.barracuda.engine.definition.DefinitionCommand.ParallelNodeDefinitionCommand;
import com.barracuda.engine.definition.DefinitionCommand.ParallelNodeDefinitionCommand.AddSubflow;
import com.barracuda.engine.definition.DefinitionCommand.ParallelNodeDefinitionCommand.RemoveSubflowCommand;
import com.barracuda.engine.definition.DefinitionEvent.FlowDefinitionEvent;
import com.barracuda.engine.definition.DefinitionEvent.ParallelNodeDefinitionEvent;
import com.barracuda.engine.definition.DefinitionEvent.ParallelNodeDefinitionEvent.SubflowAddedEvent;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@ToString
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class ParallelNodeDefinition implements NodeDefinition{

    @Getter
    private final List<FlowDefinition> subflows = new ArrayList<>();

    @EqualsAndHashCode.Include
    private final long nodeID;

    public ParallelNodeDefinition(long nodeID) {
        this.nodeID = nodeID;
    }


    @Override
    public long nodeID() {
        return nodeID;
    }

    @Override
    public void command(DefinitionCommand command) {
        if(!(command instanceof ParallelNodeDefinitionCommand parallelNodeDefinitionCommand) || parallelNodeDefinitionCommand.targetParallelNodeDefinition() != nodeID){
            subflows.forEach(flowDefinition -> flowDefinition.command(List.of(command)));
            return;
        }

        switch (parallelNodeDefinitionCommand) {
            case AddSubflow(_, long subflowID) -> subflows.add(new FlowDefinition(subflowID));
            case RemoveSubflowCommand(_, long subflowID) -> subflows.removeIf( subflow -> subflow.nodeID() == subflowID);
        }
    }

    @Override
    public void event(DefinitionEvent event) {

        if( !(event instanceof ParallelNodeDefinitionEvent parallelNodeDefinitionEvent) || parallelNodeDefinitionEvent.targetParallelNodeDefinition() != nodeID){
            //need propagation
            return;
        }

        switch (parallelNodeDefinitionEvent) {
            case SubflowAddedEvent (_, long subflowID) -> subflows.add(new FlowDefinition(subflowID));
        }

    }
}

