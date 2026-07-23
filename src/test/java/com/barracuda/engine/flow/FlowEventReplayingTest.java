package com.barracuda.engine.flow;

import com.barracuda.engine.test.flow.TestFlow;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static com.barracuda.engine.test.builder.TestFlowBuilder.testFlow;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class FlowEventReplayingTest {

    @Test
    void shouldThrowISEWhenStartEventsComesTwice() {
        assertThatThrownBy(() -> testFlow().build().sendStartEvent().sendStartEvent()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldNotAllowSendingEventsToFlowThatHasCompleted() {
        testFlow()
                .build()
                .startFlowIgnoreIfCompleted()
                .expectFlowCompleted()
                .assertThrows(TestFlow::sendStartEvent,assertError -> assertError.isInstanceOf(IllegalStateException.class).hasMessageContaining("Flow cannot accept events"));
        //add more assertions for other events here
    }

    //Note that this test might need a rework when distributed implementation is out because then RUNNING flow should be able to accept an interrupt event while it's running.
    @Test
    void shouldNotAllowSendingEventsToFlowThatIsRunning() {
        testFlow()
                .task("task")
                .build()
                .startFlow()
                .assertThrows(TestFlow::sendStartEvent,assertError -> assertError.isInstanceOf(IllegalStateException.class).hasMessageContaining("Flow cannot accept events"));
    }

    @Test
    void shouldNotAllowSendingEventsToFailedFlow() {
        testFlow()
                .task("task")
                .build()
                .startFlow()
                .failTask("task", new RuntimeException("FAILED"))
                .expectFlowFailed()
                .assertThrows(TestFlow::sendStartEvent, assertError -> assertError.isInstanceOf(IllegalStateException.class).hasMessageContaining("Flow cannot accept events"));
    }

}
