package com.barracuda.engine.flow;

import com.barracuda.engine.test.ParallelTestTask;
import com.barracuda.engine.utility.AwaitilityUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import static com.barracuda.engine.test.TestFlowBuilder.testFlow;

/**
 * Tests related to parallel tasks.
 */
public class FlowParallelTasksTest extends AbstractFlowTest{

    @Test
    void shouldAllowExecutingTasksInParallelWithSubWorkflows() {
        var readinessLatch = new CountDownLatch(3);
        var barrierLatch = new CountDownLatch(1);

        var flow = rootFlowBuilder
                .parallel(parallel ->
                        parallel
                                .subflow(subflow -> subflow.ioTask(new ParallelTestTask(readinessLatch, barrierLatch, 1L)).withID(1L))
                                .subflow(subflow -> subflow.ioTask(new ParallelTestTask(readinessLatch, barrierLatch, 2L)).withID(2L))
                                .subflow(subflow -> subflow.ioTask(new ParallelTestTask(readinessLatch, barrierLatch, 3L)).withID(3L))
                ).build();

        ioTaskExecutor.submit(flow::execute);

        Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(readinessLatch::await);

        barrierLatch.countDown();

        AwaitilityUtils.waitUntilFlowCompleted(flow);
    }

    @Test
    void flowShouldHaveFailedStateIfParallelSubflowFailsWithException() {
        var exception = new RuntimeException("FAILED");
        testFlow()
                .parallel("ParallelFailTask","ParallelTask2","ParallelTask3")
                .build()
                .startFlow()
                .assertTaskRunning("ParallelFailTask")
                .assertTaskRunning("ParallelTask2")
                .assertTaskRunning("ParallelTask3")
                .failTask("ParallelFailTask",exception)
                .expectFlowFailed(exception);
    }

    @Test
    void shouldCancelParallelSubflowsIfOneOfThemFails() {
        testFlow()
                .parallel("ParallelFailTask","ParallelTask2","ParallelTask3")
                .build()
                .startFlow()
                .failTask("ParallelFailTask",new RuntimeException("FAILED"))
                .assertTaskCancelled("ParallelTask2")
                .assertTaskCancelled("ParallelTask3");

    }

    @Test
    void shouldNotRunNextTaskWhenParallelSubflowFails() {
        testFlow()
                .parallel("ParallelTask1")
                .task("NextTask")
                .build()
                .startFlow()
                .failTask("ParallelTask1",new RuntimeException("FAILED"))
                .assertTaskNotStarted("NextTask");

    }

    @Test
    void shouldExecuteTheNextTaskWhenParallelSubflowsComplete() {
        testFlow()
                .parallel("ParallelTask1","ParallelTask2")
                .task("NextTask")
                .build()
                .startFlow()
                .finishTask("ParallelTask1")
                .assertTaskNotStarted("NextTask")
                .finishTask("ParallelTask2")
                .assertTaskRunning("NextTask");
    }

}
