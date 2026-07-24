package com.barracuda.engine.flow;

import com.barracuda.engine.event.ExecutionEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent;
import com.barracuda.engine.test.flow.SubflowEventsInOrderVerifier;
import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

import static com.barracuda.engine.test.builder.TestFlowBuilder.testFlow;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests related to flow events.
 */
public class FlowEventsTest {

    @Test
    void shouldPublishFlowStartedEventWhenStartingTheFlow() {
        testFlow()
                .ioTask("test")
                .build()
                .startFlow()
                .assertFlowEventsInOrder(events -> events.hasFlowStartedEvent().andHasNoMoreEvents());
    }

    @Test
    void shouldPublishFlowCompletedEventWhenFlowFinishesNormally() {
        testFlow()
                .ioTask("test")
                .build()
                .startFlow()
                .finishTask("test")
                .expectFlowCompleted()
                .assertFlowEventsInOrder(events -> events.hasFlowStartedEvent().hasFlowCompletedEvent().andHasNoMoreEvents());
    }

    @Test
    void shouldPublishFlowFailedEventWhenATaskFailsWithAnException() {
        var exception = new RuntimeException("FAILED");

        testFlow()
                .ioTask("test")
                .build()
                .startFlow()
                .failTask("test", exception)
                .expectFlowFailed()
                .assertFlowEventsInOrder(events -> events
                        .hasFlowStartedEvent()
                        .hasFlowFailedEvent(event -> assertThat(event.exception()).isSameAs(exception))
                        .andHasNoMoreEvents()
                );
    }

    @Test
    void shouldPublishFlowPausedEventWhenInterrupted() {
        //need better output for debugging/testing.
        testFlow()
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
                .finishTask("parallelTask1")
                .interruptFlowAndExpectFlowPaused()
                .assertFlowEventsInOrder(events -> events.hasFlowStartedEvent().hasFlowPausedEvent().andHasNoMoreEvents());
    }

    @Test
    void shouldPublishSubflowStartedEvent() {
        testFlow()
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
                .assertTaskRunning("parallelTask1")
                .finishTask("parallelTask1")
                .interruptFlowAndExpectFlowPaused()
                .assertSubflowEventsInOrder("Subflow1", SubflowEventsInOrderVerifier::hasSubflowStartedEvent)
                .assertSubflowEventsInOrder("Subflow2", SubflowEventsInOrderVerifier::hasSubflowStartedEvent)
                .assertSubflowEventsInOrder("Subflow3", SubflowEventsInOrderVerifier::hasSubflowStartedEvent);
    }

    @Test
    void shouldPublishSubflowCompletedEventForSubflowThatCompleted() {
        testFlow()
                .parallel(parallel -> parallel
                        .subflow("Subflow1", subflow -> subflow.ioTask("parallelTask1"))
                        .subflow("Subflow2", subflow -> subflow.ioTask("parallelTask2"))
                        .subflow("Subflow3", subflow -> subflow.ioTask("parallelTask3"))
                )
                .build()
                .startFlow()
                .finishTask("parallelTask1")
                .finishTask("parallelTask2")
                .expectTaskIsRunning("parallelTask3")
                .assertSubflowEventsInOrder("Subflow1", events -> events.hasSubflowStartedEvent().hasSubflowCompletedEvent().andHasNoMoreEvents())
                .assertSubflowEventsInOrder("Subflow2", events -> events.hasSubflowStartedEvent().hasSubflowCompletedEvent().andHasNoMoreEvents())
                .assertSubflowEventsInOrder("Subflow3", events -> events.hasSubflowStartedEvent().andHasNoMoreEvents());
    }

    @Test
    void shouldPublishSubflowFailedEvent() {
        testFlow()
                .parallel(parallel -> parallel
                        .subflow("Subflow1", subflow -> subflow.ioTask("parallelTask1"))
                        .subflow("Subflow2", subflow -> subflow.ioTask("parallelTask2"))
                        .subflow("Subflow3", subflow -> subflow.ioTask("parallelTask3"))
                )
                .build()
                .startFlow()
                .failTask("parallelTask1",new RuntimeException("FAILED"))
                .assertSubflowEventsInOrder("Subflow1",events -> events
                        .hasSubflowStartedEvent()
                        .hasSubflowFailedEvent(event -> assertThat(event.exception()).isInstanceOf(RuntimeException.class).hasMessageContaining("FAILED"))
                        .andHasNoMoreEvents()
                );
    }

    @Test
    void shouldPublishSubflowInterruptedEventWhenOneParallelSubflowFails() {
        testFlow()
                .parallel(parallel -> parallel
                        .subflow("Subflow1", subflow -> subflow.ioTask("parallelTask1"))
                        .subflow("Subflow2", subflow -> subflow.ioTask("parallelTask2"))
                        .subflow("Subflow3", subflow -> subflow.ioTask("parallelTask3"))
                )
                .build()
                .startFlow()
                .failTask("parallelTask1",new RuntimeException("FAILED"))
                .assertSubflowEventsInOrder("Subflow2", events -> events.hasSubflowStartedEvent().hasSubflowPausedEvent().andHasNoMoreEvents())
                .assertSubflowEventsInOrder("Subflow3", events -> events.hasSubflowStartedEvent().hasSubflowPausedEvent().andHasNoMoreEvents());
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
                .expectTaskIsRunning("task1")
                .finishTask("task1")
                .expectTaskIsRunning("task2")
                .finishTask("task2")
                .expectTaskIsRunning("task3")
                .finishTask("task3")
                .expectFlowCompleted();

        record SubflowAndEvent(String subflowName, SubflowEvent event){ }

        Stream.of("Subflow1","Subflow2","Subflow3")
                .flatMap( subflow -> flow.subflowEvents(subflow).stream().map(event -> new SubflowAndEvent(subflow,event)))
                .forEach( result -> {
                    assertThat(result.event.rootID()).isEqualTo(flow.flowID());
                    assertThat(result.event.subflowID()).isEqualTo(flow.subflowID(result.subflowName));
                });
    }
}
