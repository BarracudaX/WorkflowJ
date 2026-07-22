package com.barracuda.engine.flow;

import com.barracuda.engine.event.ExecutionEvent.ContinueEvent;
import com.barracuda.engine.test.task.ParallelTestTask;
import com.barracuda.engine.test.flow.TestSubflow;
import com.barracuda.engine.utility.AwaitilityUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.barracuda.engine.test.builder.TestFlowBuilder.testFlow;

/**
 * Tests related to parallel tasks.
 */
public class FlowParallelTasksTest extends AbstractFlowTest{

    @Test
    void shouldAllowExecutingTasksInParallelWithSubWorkflows() {
        var readinessLatch = new CountDownLatch(3);
        var barrierLatch = new CountDownLatch(1);

        var flow = flowBuilder
                .parallel(parallel ->
                        parallel
                                .subflow(subflow -> subflow.ioTask(new ParallelTestTask(readinessLatch, barrierLatch, 1L)).withID(1L))
                                .subflow(subflow -> subflow.ioTask(new ParallelTestTask(readinessLatch, barrierLatch, 2L)).withID(2L))
                                .subflow(subflow -> subflow.ioTask(new ParallelTestTask(readinessLatch, barrierLatch, 3L)).withID(3L))
                ).build();

        ioTaskExecutor.submit(() -> flow.event(new ContinueEvent(flow.id())));

        Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(readinessLatch::await);

        barrierLatch.countDown();

        AwaitilityUtils.waitUntilFlowCompleted(flow, Duration.ofSeconds(1));
    }

    @Test
    void flowShouldHaveFailedStateIfParallelSubflowFailsWithException() {
        var exception = new RuntimeException("FAILED");
        testFlow()
                .subflows(new TestSubflow( "Subflow1", List.of("ParallelFailTask")),new TestSubflow( "Subflow2", List.of("ParallelTask2")),new TestSubflow( "Subflow3", List.of("ParallelTask3")))
                .build()
                .startFlow()
                .assertTaskRunning("ParallelFailTask")
                .assertTaskRunning("ParallelTask2")
                .assertTaskRunning("ParallelTask3")
                .failTask("ParallelFailTask",exception)
                .expectFlowFailed(exception);
    }

    @Test
    void shouldCancelParallelTasksOfASubflowIfOneOfThemFails() {
        testFlow()
                .subflows(new TestSubflow( "Subflow1", List.of("ParallelTask1")),new TestSubflow( "Subflow2", List.of("ParallelTask2")),new TestSubflow( "Subflow3", List.of("ParallelTask3")))
                .build()
                .startFlow()
                .assertTaskRunning("ParallelTask1")
                .assertTaskRunning("ParallelTask2")
                .assertTaskRunning("ParallelTask3")
                .failTask("ParallelTask1",new RuntimeException("FAILED"))
                .assertTaskCancelled("ParallelTask2")
                .assertTaskCancelled("ParallelTask3");

    }

    @Test
    void shouldNotRunNextTaskWhenParallelSubflowFails() {
        testFlow()
                .subflows(new TestSubflow( "Subflow1", List.of("ParallelTask1")))
                .task("NextTask")
                .build()
                .startFlow()
                .failTask("ParallelTask1",new RuntimeException("FAILED"))
                .assertTaskNotStarted("NextTask");

    }

    @Test
    void shouldExecuteTheNextTaskWhenParallelSubflowsComplete() {
        testFlow()
                .subflows(new TestSubflow( "Subflow1", List.of("ParallelTask1")))
                .subflows(new TestSubflow( "Subflow2", List.of("ParallelTask2")))
                .task("NextTask")
                .build()
                .startFlow()
                .finishTask("ParallelTask1")
                .assertTaskNotStarted("NextTask")
                .finishTask("ParallelTask2")
                .assertTaskRunning("NextTask");
    }

}
