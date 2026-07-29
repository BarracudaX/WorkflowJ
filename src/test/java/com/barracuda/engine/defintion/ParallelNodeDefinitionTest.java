package com.barracuda.engine.defintion;

import com.barracuda.engine.definition.*;
import com.barracuda.engine.definition.DefinitionCommand.ParallelNodeDefinitionCommand.AddSubflow;
import com.barracuda.engine.definition.DefinitionCommand.ParallelNodeDefinitionCommand.RemoveSubflowCommand;
import com.barracuda.engine.definition.DefinitionEvent.ParallelNodeDefinitionEvent.SubflowAddedEvent;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
public class ParallelNodeDefinitionTest {

    private final long parallelNodeID = 1L;
    private final ParallelNodeDefinition parallelNode = new ParallelNodeDefinition(parallelNodeID);

    @Test
    void shouldSupportAddingSubflowToParallelNodeWithCommand() {
        parallelNode.command(new AddSubflow(parallelNodeID,4L));
        parallelNode.command(new AddSubflow(parallelNodeID,5L));

        assertParallelNodeContainsSubflows(parallelNode,List.of(4L,5L));
    }

    @Test
    void shouldSupportRemovingSubflowFromParallelNodeWithCommand() {
        parallelNode.command(new AddSubflow(parallelNodeID,4L));
        parallelNode.command(new AddSubflow(parallelNodeID,5L));

        parallelNode.command(new RemoveSubflowCommand(parallelNodeID, 4L));

        assertParallelNodeContainsSubflows(parallelNode,List.of(5L));
    }


    @Test
    void shouldSupportSubflowAddedEvent() {
        assertThat(parallelNode.getSubflows()).isEmpty();

        parallelNode.event(new SubflowAddedEvent(parallelNodeID,4L));
        parallelNode.event(new SubflowAddedEvent(parallelNodeID,5L));

        assertParallelNodeContainsSubflows(parallelNode,List.of(4L,5L));
    }

    @Test
    void shouldIgnoreSubflowAddCommandIfTargetNodeIsDifferent() {
        parallelNode.command(new AddSubflow(parallelNodeID,4L));

        //ignored
        parallelNode.command(new AddSubflow(parallelNodeID+10,5L));
        parallelNode.command(new AddSubflow(parallelNodeID+10, 6L));

        assertParallelNodeContainsSubflows(parallelNode,List.of(4L));
    }

    @Test
    void shouldIgnoreSubflowAddedEventIfTargetNodeIsDifferent() {
        parallelNode.event(new SubflowAddedEvent(parallelNodeID,4L));

        //ignored
        parallelNode.event(new SubflowAddedEvent(parallelNodeID+10,4L));
        parallelNode.event(new SubflowAddedEvent(parallelNodeID+10, 5L));

        assertParallelNodeContainsSubflows(parallelNode,List.of(4L));
    }

    private void assertParallelNodeContainsSubflows(ParallelNodeDefinition node,List<Long> subflows) {
        assertThat(node.getSubflows())
                .extracting(FlowDefinition::nodeID)
                .containsExactlyElementsOf(subflows);
    }
}
