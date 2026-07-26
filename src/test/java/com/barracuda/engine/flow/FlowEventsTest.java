package com.barracuda.engine.flow;

import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowCompletedEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowFailedEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowPausedEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowStartedEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.SubflowCompletedEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.SubflowFailedEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.SubflowStartedEvent;
import com.barracuda.engine.test.flow.TestFlow;
import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

import static com.barracuda.engine.test.assertJ.CustomAssertions.assertThat;
import static com.barracuda.engine.test.builder.TestFlowBuilder.testFlow;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests related to flow events.
 */
public class FlowEventsTest {

    @Test
    void shouldPublishFlowStartedEventWhenStartingTheFlow() {
        TestFlow testFlow = testFlow()
                .ioTask("test")
                .build()
                .startFlow();

        assertThat(testFlow).flowEventsSatisfying(events -> events.nextEventIs(FlowStartedEvent.class).andHasNoMoreEvents());
    }

    @Test
    void shouldPublishFlowCompletedEventWhenFlowFinishesNormally() {
        TestFlow testFlow = testFlow()
                .ioTask("test")
                .build()
                .startFlow()
                .finishTask("test")
                .waitUntilFlowCompleted();

        assertThat(testFlow).flowEventsSatisfying(events -> events.nextEventIs(FlowStartedEvent.class).nextEventIs(FlowCompletedEvent.class).andHasNoMoreEvents());
    }

    @Test
    void shouldPublishFlowFailedEventWhenATaskFailsWithAnException() {
        var exception = new RuntimeException("FAILED");

        TestFlow testFlow = testFlow()
                .ioTask("test")
                .build()
                .startFlow()
                .failTask("test", exception);

        assertThat(testFlow).flowEventsSatisfying(events ->
                events.nextEventIs(FlowStartedEvent.class)
                        .nextEventIs(FlowFailedEvent.class, event -> assertThat(event.exception()).isEqualTo(exception))
                        .andHasNoMoreEvents()
        );
    }

    @Test
    void shouldPublishFlowPausedEventWhenInterrupted() {
        TestFlow testFlow = testFlow()
                .ioTask("task1")
                .parallel(parallel -> parallel
                        .subflow("Subflow1", subflow -> subflow.ioTask("parallelTask1"))
                        .subflow("Subflow2", subflow -> subflow.ioTask("parallelTask2"))
                        .subflow("Subflow3", subflow -> subflow.ioTask("parallelTask3"))
                )
                .ioTask("task2")
                .build()
                .startFlow()
                .waitUntilTaskRunning("task1")
                .finishTask("task1")
                .waitUntilTaskRunning("parallelTask1")
                .finishTask("parallelTask1")
                .interruptFlow()
                .waitUntilPaused();

        assertThat(testFlow).flowEventsSatisfying(events -> events.nextEventIs(FlowStartedEvent.class).nextEventIs(FlowPausedEvent.class).andHasNoMoreEvents());
    }

    @Test
    void shouldPublishSubflowStartedEvent() {
        TestFlow testFlow = testFlow()
                .ioTask("task1")
                .parallel(parallel -> parallel
                        .subflow("Subflow1", subflow -> subflow.ioTask("parallelTask1"))
                        .subflow("Subflow2", subflow -> subflow.ioTask("parallelTask2"))
                        .subflow("Subflow3", subflow -> subflow.ioTask("parallelTask3"))
                )
                .ioTask("task2")
                .build()
                .startFlow()
                .finishTask("task1")
                .waitUntilTaskRunning("parallelTask1");

        assertThat(testFlow).subflowEventsSatisfying("Subflow1", events -> events.nextEventIs(SubflowStartedEvent.class).andHasNoMoreEvents());
        assertThat(testFlow).subflowEventsSatisfying("Subflow2", events -> events.nextEventIs(SubflowStartedEvent.class).andHasNoMoreEvents());
        assertThat(testFlow).subflowEventsSatisfying("Subflow3", events -> events.nextEventIs(SubflowStartedEvent.class).andHasNoMoreEvents());
    }

    @Test
    void shouldPublishSubflowCompletedEventForSubflowThatCompleted() {
        TestFlow testFlow = testFlow()
                .parallel(parallel -> parallel
                        .subflow("Subflow1", subflow -> subflow.ioTask("parallelTask1"))
                        .subflow("Subflow2", subflow -> subflow.ioTask("parallelTask2"))
                        .subflow("Subflow3", subflow -> subflow.ioTask("parallelTask3"))
                )
                .build()
                .startFlow()
                .waitUntilTaskRunning("parallelTask1")
                .waitUntilTaskRunning("parallelTask2")
                .waitUntilTaskRunning("parallelTask3")
                .finishTask("parallelTask1")
                .finishTask("parallelTask2");

        assertThat(testFlow)
                .subflowEventsSatisfying("Subflow1", events ->
                        events
                                .nextEventIs(SubflowStartedEvent.class)
                                .nextEventIs(SubflowCompletedEvent.class)
                                .andHasNoMoreEvents()
                ).subflowEventsSatisfying("Subflow2", events ->
                        events
                                .nextEventIs(SubflowStartedEvent.class)
                                .nextEventIs(SubflowCompletedEvent.class)
                                .andHasNoMoreEvents()
                ).subflowEventsSatisfying("Subflow3", events ->
                        events
                                .nextEventIs(SubflowStartedEvent.class)
                                .andHasNoMoreEvents()
                );

    }

    @Test
    void shouldPublishSubflowFailedEvent() {
        RuntimeException exception = new RuntimeException("FAILED");
        TestFlow testFlow = testFlow()
                .parallel(parallel -> parallel.subflow("Subflow1", subflow -> subflow.ioTask("parallelTask1")))
                .build()
                .startFlow()
                .waitUntilTaskRunningAndFailItAndWaitUntilFailed("parallelTask1", exception);

        assertThat(testFlow)
                .subflowEventsSatisfying("Subflow1", events ->
                        events
                                .nextEventIs(SubflowStartedEvent.class)
                                .nextEventIs(SubflowFailedEvent.class, event -> assertThat(event.exception()).isEqualTo(exception))
                                .andHasNoMoreEvents()
                );
    }

    @Test
    void shouldPublishSubflowInterruptedEventWhenOneParallelSubflowFails() {
        RuntimeException exception = new RuntimeException("FAILED");
        TestFlow testFlow = testFlow()
                .parallel(parallel -> parallel
                        .subflow("Subflow1", subflow -> subflow.ioTask("parallelTask1"))
                        .subflow("Subflow2", subflow -> subflow.ioTask("parallelTask2"))
                        .subflow("Subflow3", subflow -> subflow.ioTask("parallelTask3"))
                )
                .build()
                .startFlow()
                .waitUntilTaskRunningAndFailItAndWaitUntilFailed("parallelTask1", exception);

        assertThat(testFlow).subflowEventsSatisfying("Subflow1", events ->
                events
                        .nextEventIs(SubflowStartedEvent.class)
                        .nextEventIs(SubflowFailedEvent.class, event -> assertThat(event.exception()).isEqualTo(exception))
                        .andHasNoMoreEvents()
        );

        assertThat(testFlow).subflowEventsSatisfying("Subflow2", events -> events.nextEventIs(SubflowStartedEvent.class).nextEventIs(SubflowEvent.SubflowPausedEvent.class).andHasNoMoreEvents());
        assertThat(testFlow).subflowEventsSatisfying("Subflow3", events -> events.nextEventIs(SubflowStartedEvent.class).nextEventIs(SubflowEvent.SubflowPausedEvent.class).andHasNoMoreEvents());

    }

    @Test
    void subflowEventsShouldHaveCorrectRootIDOfRootFlow() {
        var flow = testFlow()
                .parallel(parallelL1 -> {
                    parallelL1.subflow("Subflow1", subflow1 -> {
                        subflow1
                                .ioTask("task1")
                                .parallel(parallelL2 -> {
                                    parallelL2.subflow("Subflow2", subflow2 -> {
                                        subflow2
                                                .ioTask("task2").
                                                parallel(parallelL3 -> {
                                                    parallelL3.subflow("Subflow3", subflow3 -> {
                                                        subflow3.ioTask("task3");
                                            });
                                        });
                                    });
                                });
                    });
                })
                .build()
                .startFlow()
                .waitUntilTaskRunning("task1")
                .finishTask("task1")
                .waitUntilTaskRunning("task2")
                .finishTask("task2")
                .waitUntilTaskRunning("task3")
                .finishTask("task3")
                .waitUntilFlowCompleted();

        record SubflowAndEvent(String subflowName, SubflowEvent event){ }

        Stream.of("Subflow1","Subflow2","Subflow3")
                .flatMap( subflow -> flow.subflowEvents(subflow).stream().map(event -> new SubflowAndEvent(subflow,event)))
                .forEach( result -> {
                    assertThat(result.event.rootID()).isEqualTo(flow.flowID());
                    assertThat(result.event.subflowID()).isEqualTo(flow.subflowID(result.subflowName));
                });
    }
}
