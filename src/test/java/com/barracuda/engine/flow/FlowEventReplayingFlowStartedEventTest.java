package com.barracuda.engine.flow;

import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowPausedEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowStartedEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.SubflowPausedEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.SubflowStartedEvent;
import com.barracuda.engine.test.flow.TestFlow;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static com.barracuda.engine.test.assertJ.CustomAssertions.assertThat;
import static com.barracuda.engine.test.builder.TestFlowBuilder.testFlow;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class FlowEventReplayingFlowStartedEventTest {

    @Test
    void shouldAllowSendingStartEventWhenInReplayMode() {
        assertThatCode(() ->
                testFlow()
                        .ioTask("task")
                        .build()
                        .enterReplayMode() /* enter replay mode */
                        .sendFlowStartedEvent()
        ).doesNotThrowAnyException();
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

    /**
     * This method is similar to shouldNotAllowSendingFlowStartEventToFlowThatIsRunning but also checks that the flow doesn't react to the event by checking the event history didn't change.
     */
    @Test
    void shouldNotReactToFlowStartedEventIfFlowIsRunning() {
        TestFlow testFlow = testFlow()
                .ioTask("task")
                .build()
                .startFlow();
        assertThat(testFlow).flowEventsSatisfy(events -> events.nextEventIs(FlowStartedEvent.class).andHasNoMoreEvents());

        assertThatThrownBy(testFlow::sendFlowStartedEvent).isInstanceOf(IllegalStateException.class).hasMessageContaining("Flow cannot accept events");

        assertThat(testFlow).flowEventsSatisfy(events -> events.nextEventIs(FlowStartedEvent.class).andHasNoMoreEvents());
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
                .sendFlowStartedEvent(); // first send is okay

        assertThatThrownBy(testFlow::sendFlowStartedEvent) // second send
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Duplicate");
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
                .startFlow() // run the flow so that the flow started event is published
                .interruptFlow();

        assertThat(testFlow).flowEventsSatisfy(events -> events.nextEventIs(FlowStartedEvent.class).nextEventIs(FlowPausedEvent.class).andHasNoMoreEvents());

        testFlow
                .reset() // reset the flow
                .enterReplayMode()
                .sendFlowStartedEvent()
                .prepare()
                .startFlow();

        //events are the same, hence no new event was published.
        assertThat(testFlow).flowEventsSatisfy(events -> events.nextEventIs(FlowStartedEvent.class).nextEventIs(FlowPausedEvent.class).andHasNoMoreEvents());
    }

    @Test
    void shouldNotAllowSendingDuplicateSubflowStartEvent() {
        var flow = testFlow()
                .parallel(parallel -> parallel.subflow("Subflow1", subflow -> subflow.ioTask("task")))
                .build()
                .startFlow()
                .interruptFlow()
                .reset()
                .enterReplayMode();

        var event = new SubflowStartedEvent(flow.flowID(), flow.subflowID("Subflow1"));

        flow.sendEvent(event); // should go through the root and reach the subflow.

        assertThatThrownBy(() -> flow.sendEvent(event)).isInstanceOf(IllegalStateException.class).hasMessageContaining("Duplicate");

    }

    @Test
    void shouldNotPublishSubflowStartEventWhenItHasBeenReplayed() {
        var flow = testFlow()
                .parallel(parallel -> parallel.subflow("Subflow1", subflow -> subflow.ioTask("task")))
                .build()
                .startFlow()
                .interruptFlow()
                .reset()
                .enterReplayMode();
        assertThat(flow).subflowEventsSatisfy("Subflow1", events -> events.nextEventIs(SubflowStartedEvent.class).nextEventIs(SubflowPausedEvent.class).andHasNoMoreEvents());

        flow
                .sendEvent(new SubflowStartedEvent(flow.flowID(), flow.subflowID("Subflow1")))
                .prepare()
                .startFlow();

        assertThat(flow).subflowEventsSatisfy("Subflow1", events -> events.nextEventIs(SubflowStartedEvent.class).nextEventIs(SubflowPausedEvent.class).andHasNoMoreEvents());
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
