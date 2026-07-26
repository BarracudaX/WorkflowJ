package com.barracuda.engine.flow;

import com.barracuda.engine.event.ExecutionEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowPausedEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowStartedEvent;
import com.barracuda.engine.test.flow.TestFlow;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static com.barracuda.engine.test.assertJ.CustomAssertions.assertThat;
import static com.barracuda.engine.test.builder.TestFlowBuilder.testFlow;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class FlowEventReplayingStartEventTest {

    //How can we verify that the flow processed the event?
    @Test
    void shouldAllowSendingStartEventWhenInReplayMode() {
        testFlow()
                .ioTask("task")
                .build()
                .enterReplayMode() /* enter replay mode */
                .sendFlowStartedEvent();
    }

    @Test
    void shouldNotAllowSendingFlowStartEventToFlowThatHasCompleted() {
        TestFlow testFlow = testFlow()
                .ioTask("task")
                .build()
                .startFlow()
                .finishTask("task")
                .waitUntilFlowCompleted();

        assertThatThrownBy(testFlow::sendFlowStartedEvent).isInstanceOf(IllegalStateException.class).hasMessageContaining("Flow cannot accept events");
    }

    @Test
    void shouldNotAllowSendingFlowStartEventToFlowThatIsRunning() {
        TestFlow testFlow = testFlow()
                .ioTask("task")
                .build()
                .startFlow();

        assertThatThrownBy(testFlow::sendFlowStartedEvent).isInstanceOf(IllegalStateException.class).hasMessageContaining("Flow cannot accept events");
    }

    @Test
    void shouldNotAllowSendingFlowStartEventToFailedFlow() {
        TestFlow testFlow = testFlow()
                .ioTask("task")
                .build()
                .startFlow()
                .failTask("task", new RuntimeException("FAILED"));

        assertThatThrownBy(testFlow::sendFlowStartedEvent).isInstanceOf(IllegalStateException.class).hasMessageContaining("Flow cannot accept events");
    }

    @Test
    void shouldNotAllowSendingFlowStartEventToFlowThatIsInReadyState() {
        TestFlow testFlow = testFlow()
                .ioTask("task")
                .build();

        assertThatThrownBy(testFlow::sendFlowStartedEvent).isInstanceOf(IllegalStateException.class).hasMessageContaining("Flow cannot accept events");
    }

    @Test
    void shouldNotAllowSendingEventsToPausedFlow() {
        TestFlow testFlow = testFlow()
                .ioTask("task")
                .build()
                .startFlow()
                .interruptFlow()
                .waitUntilPaused();

        assertThatThrownBy(testFlow::sendFlowStartedEvent).isInstanceOf(IllegalStateException.class).hasMessageContaining("Flow cannot accept events");
    }

    @Test
    void shouldNotAllowSendingDuplicateStartEventToFlow() {
        TestFlow testFlow = testFlow()
                .ioTask("task")
                .build()
                .enterReplayMode() /* enter replay mode */
                .sendFlowStartedEvent();

        assertThatThrownBy(testFlow::sendFlowStartedEvent).isInstanceOf(IllegalStateException.class).hasMessageContaining("Duplicate");
    }

    @Test
    void shouldIgnoreFlowStartEventIfItIsNotAssociatedWithTheFlow() {
        TestFlow flow = testFlow()
                .ioTask("task")
                .build()
                .enterReplayMode()
                .sendFlowStartedEvent();

        var unrelatedStartFlowEvent = new FlowStartedEvent(flow.flowID() + 1);

        // because start event was already sent, second one should throw an exception. But because it's unrelated, it doesn't. Need a better way to figure out if the event was handled or not.
        assertThatCode(() -> flow.sendEvent(unrelatedStartFlowEvent)).doesNotThrowAnyException();
    }

    @Test
    void shouldNotPublishFlowStartEventWhenItHasBeenReplayed() {
        TestFlow testFlow = testFlow()
                .ioTask("task")
                .build()
                .startFlow()
                .interruptFlow();

        assertThat(testFlow).flowEventsSatisfying(events -> events.nextEventIs(FlowStartedEvent.class).nextEventIs(FlowPausedEvent.class).andHasNoMoreEvents());

        testFlow
                .reset()
                .enterReplayMode()
                .sendFlowStartedEvent()
                .prepare()
                .startFlow();

        assertThat(testFlow).flowEventsSatisfying(events -> events.nextEventIs(FlowStartedEvent.class).nextEventIs(FlowPausedEvent.class).andHasNoMoreEvents());
    }

    @Disabled("TODO")
    @Test
    void shouldSkipCompletedTaskWhenReplaying() {

    }
    @Disabled("TODO")
    @Test
    void shouldContinueTheTaskIfItsCompletedEventIsNotReplayed() {

    }
    @Disabled("TODO")
    @Test
    void shouldTerminateWithExceptionWhenReplayFlowFailedEvent() {

    }
    @Disabled("TODO")
    @Test
    void shouldTerminateWithExceptionWhenReplayingSubflowFailedEvent() {

    }
}
