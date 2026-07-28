package com.barracuda.engine.flow;

import com.barracuda.engine.command.CommandRejectedException;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowPausedEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowResetEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowStartedEvent;
import com.barracuda.engine.test.flow.TestFlow;
import org.junit.jupiter.api.Test;

import static com.barracuda.engine.test.assertJ.CustomAssertions.assertThat;
import static com.barracuda.engine.test.builder.TestFlowBuilder.testFlow;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class FlowResetTest {

    @Test
    void shouldNotAllowResettingRunningFlow() {
        TestFlow testFlow = testFlow()
                .actionTask("task")
                .build()
                .startFlow();

        assertThatThrownBy(testFlow::reset).isInstanceOf(CommandRejectedException.class);
    }

    @Test
    void shouldAllowResettingCompletedFlow() {
        TestFlow testFlow = testFlow()
                .actionTask("task")
                .build()
                .startFlow()
                .finishTask("task");

        assertThat(testFlow).isEventuallyCompleted();

        testFlow.reset();

        assertThat(testFlow).isReady();
    }

    @Test
    void shouldAllowResettingFailedFlow() {
        TestFlow testFlow = testFlow()
                .actionTask("task")
                .build()
                .startFlow()
                .failTask("task", new RuntimeException("FAILED"));

        assertThat(testFlow).hasEventuallyFailed();

        testFlow.reset();

        assertThat(testFlow).isReady();
    }

    @Test
    void shouldAllowResettingPausedFlow() {
        TestFlow testFlow = testFlow()
                .actionTask("task")
                .build()
                .startFlow()
                .interruptFlow();

        assertThat(testFlow).isEventuallyPaused();

        testFlow.reset();

        assertThat(testFlow).isReady();
    }

    @Test
    void shouldSendFlowStartedEventWhenStartingFlowAgainAfterResetting() {
        TestFlow testFlow = testFlow()
                .actionTask("task")
                .build()
                .startFlow()
                .interruptFlow();

        assertThat(testFlow).isEventuallyPaused().flowEventsSatisfy(events -> events.nextEventIs(FlowStartedEvent.class).nextEventIs(FlowPausedEvent.class).andHasNoMoreEvents());

        testFlow
                .reset()
                .startFlow();

        assertThat(testFlow).flowEventsSatisfy(events -> events.nextEventIs(FlowStartedEvent.class).nextEventIs(FlowPausedEvent.class).nextEventIs(FlowResetEvent.class).nextEventIs(FlowStartedEvent.class).andHasNoMoreEvents());
    }

    @Test
    void shouldResetSubflows() {
        TestFlow testFlow = testFlow()
                .parallel(parallel -> parallel
                                .subflow("Subflow1", subflow -> subflow.actionTask("task1"))
                                .subflow("Subflow2", subflow -> subflow.actionTask("task2"))
                ).build();

        testFlow.startFlow().failTask("task2", new RuntimeException("FAILED"));

        assertThat(testFlow).hasEventuallyFailed();
        assertThat(testFlow.subflow("Subflow1")).isEventuallyPaused();
        assertThat(testFlow.subflow("Subflow2")).hasEventuallyFailed();

        testFlow.reset();

        assertThat(testFlow).isReady();
        assertThat(testFlow.subflow("Subflow1")).isReady();
        assertThat(testFlow.subflow("Subflow2")).isReady();
    }

    //test that the reset signals goes to the second level of subflow(subflow within subflow)
    @Test
    void shouldResetSubflowsLevel2(){
        TestFlow testFlow = testFlow()
                .parallel(parallel ->
                        parallel
                                .subflow("Subflow1", subflow -> subflow.actionTask("task1"))
                                .subflow("Subflow2", subflow -> subflow.parallel(parallelL2 -> parallelL2.subflow("Subflow3",subflowL2 -> subflowL2.actionTask("task2"))))
                )
                .build();

        testFlow.startFlow().failTask("task2", new RuntimeException("FAILED"));
        assertThat(testFlow).hasEventuallyFailed();

        assertThat(testFlow.subflow("Subflow1")).isEventuallyPaused();
        assertThat(testFlow.subflow("Subflow2")).hasEventuallyFailed();// failed because its child node(another subflow) has failed.
        assertThat(testFlow.subflow("Subflow3")).hasEventuallyFailed();

        testFlow.reset();

        assertThat(testFlow).isReady();
        assertThat(testFlow.subflow("Subflow1")).isReady();
        assertThat(testFlow.subflow("Subflow2")).isReady();
        assertThat(testFlow.subflow("Subflow3")).isReady();
    }
}
