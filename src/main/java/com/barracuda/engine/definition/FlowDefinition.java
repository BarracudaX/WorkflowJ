package com.barracuda.engine.definition;

import com.barracuda.engine.definition.DefinitionCommand.FlowDefinitionCommand;
import com.barracuda.engine.definition.DefinitionCommand.FlowDefinitionCommand.AddParallelNode;
import com.barracuda.engine.definition.DefinitionCommand.FlowDefinitionCommand.AddTaskNode;
import com.barracuda.engine.definition.DefinitionCommand.FlowDefinitionCommand.RemoveParallelNode;
import com.barracuda.engine.definition.DefinitionCommand.FlowDefinitionCommand.RemoveTaskNode;
import com.barracuda.engine.definition.DefinitionEvent.FlowDefinitionEvent;
import com.barracuda.engine.definition.DefinitionEvent.FlowDefinitionEvent.ParallelNodeAddedEvent;
import com.barracuda.engine.definition.DefinitionEvent.FlowDefinitionEvent.ParallelNodeRemovedEvent;
import com.barracuda.engine.definition.DefinitionEvent.FlowDefinitionEvent.TaskNodeAddedEvent;
import com.barracuda.engine.definition.DefinitionEvent.FlowDefinitionEvent.TaskNodeRemovedEvent;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString
public class FlowDefinition implements NodeDefinition{

    @EqualsAndHashCode.Include
    private final long nodeID;
    private final List<NodeDefinition> nodeDefinitions = new ArrayList<>();

    public FlowDefinition(long nodeID) {
        this.nodeID = nodeID;
    }

    public void command(List<DefinitionCommand> commands) {
        for(DefinitionCommand command : commands){
            command(command);
        }
    }

    private void propagateCommand(DefinitionCommand command) {
        nodeDefinitions.stream()
                .map( nodeDefinition -> nodeDefinition instanceof ParallelNodeDefinition parallelNode ? Optional.of(parallelNode) : Optional.<ParallelNodeDefinition>empty())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach( parallelNodeDefinition -> parallelNodeDefinition.command(command));
    }

    @Override
    public long nodeID() {
        return nodeID;
    }

    @Override
    public void command(DefinitionCommand command) {
        if ( !(command instanceof FlowDefinitionCommand flowDefinitionCommand) || flowDefinitionCommand.targetFlowDefinition() != this.nodeID) {
            propagateCommand(command);
            return;
        }

        switch (flowDefinitionCommand) {
            case AddTaskNode(long targetDefinition, long id) -> event(new TaskNodeAddedEvent(targetDefinition,id));
            case RemoveTaskNode(long targetDefinition, long id) -> event(new TaskNodeRemovedEvent(targetDefinition,id));
            case AddParallelNode(long targetDefinition, long id) -> event(new ParallelNodeAddedEvent(targetDefinition,id));
            case RemoveParallelNode(long targetDefinition, long id) -> event(new ParallelNodeRemovedEvent(targetDefinition,id));
        }
    }

    public void event(DefinitionEvent event){
        if( !(event instanceof FlowDefinitionEvent flowDefinitionEvent) || flowDefinitionEvent.targetFlowDefinition() != this.nodeID) {
            nodeDefinitions.forEach( definition -> definition.event(event));
            return;
        }

        switch (flowDefinitionEvent) {
            case TaskNodeAddedEvent (_,long id) -> nodeDefinitions.add(new TaskNodeDefinition(id));
            case TaskNodeRemovedEvent (_, long id)-> nodeDefinitions.remove(new TaskNodeDefinition(id));
            case ParallelNodeAddedEvent (_,long id) -> nodeDefinitions.add(new ParallelNodeDefinition(id));
            case ParallelNodeRemovedEvent (_, long id) -> nodeDefinitions.remove(new ParallelNodeDefinition(id));
        }
    }

    public List<NodeDefinition> nodes(){
        return nodeDefinitions;
    }

}
