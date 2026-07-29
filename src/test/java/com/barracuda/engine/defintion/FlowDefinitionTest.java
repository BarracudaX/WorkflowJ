package com.barracuda.engine.defintion;

import com.barracuda.engine.definition.*;
import com.barracuda.engine.definition.DefinitionCommand.FlowDefinitionCommand.AddParallelNode;
import com.barracuda.engine.definition.DefinitionCommand.FlowDefinitionCommand.AddTaskNode;
import com.barracuda.engine.definition.DefinitionCommand.FlowDefinitionCommand.RemoveParallelNode;
import com.barracuda.engine.definition.DefinitionCommand.FlowDefinitionCommand.RemoveTaskNode;
import com.barracuda.engine.definition.DefinitionCommand.ParallelNodeDefinitionCommand.AddSubflow;
import com.barracuda.engine.definition.DefinitionCommand.ParallelNodeDefinitionCommand.RemoveSubflowCommand;
import com.barracuda.engine.definition.DefinitionEvent.FlowDefinitionEvent.TaskNodeAddedEvent;
import com.barracuda.engine.definition.DefinitionEvent.FlowDefinitionEvent.TaskNodeRemovedEvent;
import com.barracuda.engine.definition.DefinitionEvent.ParallelNodeDefinitionEvent.SubflowAddedEvent;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.list;

public class FlowDefinitionTest {

    private static final long ROOT_FLOW_DEFINITION_ID = 1L;
    private final FlowDefinition definition = new FlowDefinition(ROOT_FLOW_DEFINITION_ID);

    @Test
    void shouldBeAbleToAddTaskNodeDefinitionToTheFlowDefinitionWithACommand() {
        assertThat(definition.nodes()).isEmpty();

        addTaskNode(1L);

        assertThat(definition.nodes()).extracting(NodeDefinition::nodeID).containsExactly(1L);
    }

    @Test
    void addedTaskNodesShouldBeOrderedByTheirInsertionOrder() {
        addTaskNode(1L);
        addTaskNode(2L);

        assertThat(definition.nodes()).extracting(NodeDefinition::nodeID).containsExactly(1L,2L);
    }

    @Test
    void shouldBeAbleToRemoveTaskNodeDefinitionFromTheFlowDefinition() {
        addTaskNode(1L);
        addTaskNode(2L);

        removeTaskNode(2L);

        assertThat(definition.nodes()).extracting(NodeDefinition::nodeID).containsExactly(1L);
    }

    @Test
    void shouldBeAbleToCreateFlowDefinitionWithEventReplaying() {
        sendTaskNodeAddedEvent(1L);
        sendTaskNodeAddedEvent(2L);

        assertThat(definition.nodes()).extracting(NodeDefinition::nodeID).containsExactly(1L,2L);
    }

    @Test
    void shouldBeAbleToRemoveTaskNodeDefinitionWithEventReplaying() {
        addTaskNode(1L);
        addTaskNode(2L);
        assertThat(definition.nodes()).containsExactly(new TaskNodeDefinition(1L), new TaskNodeDefinition(2L));

        definition.event(new TaskNodeRemovedEvent(ROOT_FLOW_DEFINITION_ID,1L));

        assertThat(definition.nodes()).extracting(NodeDefinition::nodeID).containsExactly(2L);
    }

    @Test
    void shouldSupportAddParallelNodeDefinitionCommand() {
        addTaskNode(2L);
        addParallelNode(3L);

        assertThat(definition.nodes()).extracting(NodeDefinition::nodeID).containsExactly(2L,3L);
    }

    @Test
    void shouldSupportRemoveParallelNodeDefinitionCommand() {
        addParallelNode(2L);
        addParallelNode(3L);

        removeParallelNode(3L);

        assertThat(definition.nodes()).extracting(NodeDefinition::nodeID).containsExactly(2L);
    }


    @Disabled("Need tests for adding and removing subflow")
    @Test
    void shouldSupportAddingTaskNodeDefinitionToSubflow() {

    }

    @Test
    void shouldSupportAddingTaskNodeDefinitionToSubflowUsingEventReplaying() {


    }

    private void removeParallelNode(long nodeID) {
        definition.command(new RemoveParallelNode(ROOT_FLOW_DEFINITION_ID,nodeID));
    }

    private void sendTaskNodeAddedEvent(long nodeID) {
        definition.event(new TaskNodeAddedEvent(ROOT_FLOW_DEFINITION_ID,nodeID));
    }

    private void removeTaskNode(long nodeID) {
        definition.command(List.of(new RemoveTaskNode(ROOT_FLOW_DEFINITION_ID,nodeID)));
    }

    private void addParallelNode(long id) {
        definition.command(new AddParallelNode(ROOT_FLOW_DEFINITION_ID,id));
    }

    private void addTaskNode(long id) {
        definition.command(new AddTaskNode(ROOT_FLOW_DEFINITION_ID,id));
    }



}
