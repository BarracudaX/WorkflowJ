package com.barracuda.engine.flow;

import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowPausedEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowStartedEvent;
import com.barracuda.engine.test.flow.TestFlow;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static com.barracuda.engine.test.assertJ.CustomAssertions.assertThat;
import static com.barracuda.engine.test.builder.TestFlowBuilder.testFlow;
import static org.assertj.core.api.Assertions.assertThatCode;

public class FlowResetTest {

    @Test
    void shouldNotAllowResettingRunningFlow() {
        TestFlow testFlow = testFlow()
                .ioTask("task")
                .build()
                .startFlow();

        Assertions.assertThatIllegalStateException().isThrownBy(testFlow::reset);
    }

    @Test
    void shouldAllowResettingCompletedFlow() {
        TestFlow testFlow = testFlow()
                .ioTask("task")
                .build()
                .startFlow()
                .finishTask("task")
                .waitUntilFlowCompleted();

        assertThatCode(testFlow::reset).doesNotThrowAnyException();
        assertThat(testFlow).isReady();
    }

    @Test
    void shouldAllowResettingFailedFlow() {
        TestFlow testFlow = testFlow()
                .ioTask("task")
                .build()
                .startFlow()
                .waitUntilTaskRunningAndFailItAndWaitUntilFailed("task", new RuntimeException("FAILED"))
                .reset();

        assertThat(testFlow).isReady();
    }

    @Test
    void shouldAllowResettingPausedFlow() {
        TestFlow testFlow = testFlow()
                .ioTask("task")
                .build()
                .startFlow()
                .interruptFlow()
                .reset();

        assertThat(testFlow).isReady();
    }

    @Test
    void shouldAllowResettingReplayedFlow() {
        TestFlow testFlow = testFlow()
                .ioTask("task")
                .build()
                .enterReplayMode()
                .reset();

        assertThat(testFlow).isReady();
    }

    @Test
    void shouldSendFlowStartedEventAgainAfterResetting() {
        TestFlow testFlow = testFlow()
                .ioTask("task")
                .build()
                .startFlow();

        assertThat(testFlow).flowEventsSatisfy(events -> events.nextEventIs(FlowStartedEvent.class).andHasNoMoreEvents());

        testFlow.interruptFlow().reset().startFlow(); // restart the flow

        assertThat(testFlow).flowEventsSatisfy(events -> events.nextEventIs(FlowStartedEvent.class).nextEventIs(FlowPausedEvent.class).nextEventIs(FlowStartedEvent.class).andHasNoMoreEvents());
    }

    @Test
    void shouldResetSubflows() {
        TestFlow testFlow = testFlow()
                .parallel(parallel ->
                        parallel
                                .subflow("Subflow1", subflow -> subflow.ioTask("task1"))
                                .subflow("Subflow2", subflow -> subflow.ioTask("task2"))
                )
                .build();

        testFlow.startFlow().waitUntilTaskRunningAndFailItAndWaitUntilFailed("task2", new RuntimeException("FAILED"));
        assertThat(testFlow).hasEventuallyFailed();
        assertThat(testFlow.subflow("Subflow1")).isEventuallyPaused();
        assertThat(testFlow.subflow("Subflow2")).hasEventuallyFailed();

        testFlow.reset();

        assertThat(testFlow).isReady();
        assertThat(testFlow.subflow("Subflow1")).isReady();
        assertThat(testFlow.subflow("Subflow2")).isReady();

    }

    @Test
    void shouldResetSubflowsLevel2(){
        TestFlow testFlow = testFlow()
                .parallel(parallel ->
                        parallel
                                .subflow("Subflow1", subflow -> subflow.ioTask("task1"))
                                .subflow("Subflow2", subflow -> subflow.parallel(parallelL2 -> parallelL2.subflow("Subflow3",subflowL2 -> subflowL2.ioTask("task2"))))
                )
                .build();

        testFlow.startFlow().waitUntilTaskRunningAndFailItAndWaitUntilFailed("task2", new RuntimeException("FAILED"));
        assertThat(testFlow).hasEventuallyFailed();
        assertThat(testFlow.subflow("Subflow1")).isEventuallyPaused();
        assertThat(testFlow.subflow("Subflow2")).hasEventuallyFailed();
        assertThat(testFlow.subflow("Subflow3")).hasEventuallyFailed();

        testFlow.reset();

        assertThat(testFlow).isReady();
        assertThat(testFlow.subflow("Subflow1")).isReady();
        assertThat(testFlow.subflow("Subflow2")).isReady();
        assertThat(testFlow.subflow("Subflow3")).isReady();
    }
}
