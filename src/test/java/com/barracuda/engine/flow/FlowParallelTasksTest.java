package com.barracuda.engine.flow;

import com.barracuda.engine.event.ExecutionEvent.CommandEvent.Continue;
import com.barracuda.engine.test.assertJ.CustomAssertions;
import com.barracuda.engine.test.assertJ.TestFlowAssert;
import com.barracuda.engine.test.assertJ.TestFlowAssert.TestTaskAssert;
import com.barracuda.engine.test.flow.TestFlow;
import com.barracuda.engine.test.task.ParallelTestTask;
import com.barracuda.engine.utility.AwaitilityUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import static com.barracuda.engine.test.assertJ.CustomAssertions.assertThat;
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
                                .subflow(2L,subflow -> subflow.ioTask(new ParallelTestTask(readinessLatch, barrierLatch, 1L)))
                                .subflow(3L,subflow -> subflow.ioTask(new ParallelTestTask(readinessLatch, barrierLatch, 2L)))
                                .subflow(4L,subflow -> subflow.ioTask(new ParallelTestTask(readinessLatch, barrierLatch, 3L)))
                ).build();

        ioTaskExecutor.submit(() -> flow.event(new Continue()));

        Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(readinessLatch::await);

        barrierLatch.countDown();

        AwaitilityUtils.waitUntilFlowCompleted(flow, Duration.ofSeconds(1));
    }

    @Test
    void flowShouldHaveFailedStateIfParallelSubflowFailsWithException() {
        var exception = new RuntimeException("FAILED");
        TestFlow testFlow = testFlow()
                .parallel(parallel -> parallel
                        .subflow("Subflow1", subflow -> subflow.ioTask("ParallelFailTask"))
                        .subflow("Subflow2", subflow -> subflow.ioTask("ParallelTask2"))
                        .subflow("Subflow3", subflow -> subflow.ioTask("ParallelTask3"))
                )
                .build()
                .startFlow()
                .waitUntilTaskRunning("ParallelTask2")
                .waitUntilTaskRunning("ParallelTask3")
                .failTask("ParallelFailTask", exception);

        assertThat(testFlow).hasEventuallyFailedWith(exception);
    }

    @Test
    void shouldCancelParallelTasksOfASubflowIfOneOfThemFails() {
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
                .failTask("parallelTask1", new RuntimeException("FAILED"));

        assertThat(testFlow).hasTaskSatisfying("parallelTask2", TestTaskAssert::isEventuallyCancelled);
        assertThat(testFlow).hasTaskSatisfying("parallelTask3", TestTaskAssert::isEventuallyCancelled);
    }

    @Test
    void shouldNotRunNextTaskWhenParallelSubflowFails() {
        TestFlow testFlow = testFlow()
                .parallel(parallel -> parallel.subflow("Subflow1", subflow -> subflow.ioTask("parallelTask1")))
                .ioTask("NextTask")
                .build()
                .startFlow()
                .failTask("parallelTask1", new RuntimeException("FAILED"));

        assertThat(testFlow).hasTaskSatisfying("NextTask", TestTaskAssert::hasNotStarted);
    }

    @Test
    void shouldExecuteTheNextTaskWhenParallelSubflowsComplete() {
        TestFlow testFlow = testFlow()
                .parallel(parallel -> parallel.subflow("Subflow1", subflow -> subflow.ioTask("parallelTask1")))
                .parallel(parallel -> parallel.subflow("Subflow2", subflow -> subflow.ioTask("parallelTask2")))
                .ioTask("NextTask")
                .build()
                .startFlow()
                .finishTask("parallelTask1")
                .finishTask("parallelTask2");

        assertThat(testFlow).hasTaskSatisfying("NextTask", TestTaskAssert::isRunning);

    }

}
