package com.barracuda.engine.flow;

import com.barracuda.engine.event.ExecutionEvent.CommandEvent.Continue;
import com.barracuda.engine.test.task.ParallelTestTask;
import com.barracuda.engine.utility.AwaitilityUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;
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
        testFlow()
                .parallel(parallel -> parallel
                        .subflow("Subflow1", subflow -> subflow.ioTask("ParallelFailTask"))
                        .subflow("Subflow2", subflow -> subflow.ioTask("ParallelTask2"))
                        .subflow("Subflow3", subflow -> subflow.ioTask("ParallelTask3"))
                )
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
                .parallel(parallel -> parallel
                        .subflow("Subflow1", subflow -> subflow.ioTask("parallelTask1"))
                        .subflow("Subflow2", subflow -> subflow.ioTask("parallelTask2"))
                        .subflow("Subflow3", subflow -> subflow.ioTask("parallelTask3"))
                )
                .build()
                .startFlow()
                .assertTaskRunning("parallelTask1")
                .assertTaskRunning("parallelTask2")
                .assertTaskRunning("parallelTask3")
                .failTask("parallelTask1",new RuntimeException("FAILED"))
                .expectTaskCancelled("parallelTask2")
                .expectTaskCancelled("parallelTask3");

    }

    @Test
    void shouldNotRunNextTaskWhenParallelSubflowFails() {
        testFlow()
                .parallel(parallel -> parallel.subflow("Subflow1", subflow -> subflow.ioTask("parallelTask1")))
                .ioTask("NextTask")
                .build()
                .startFlow()
                .failTask("parallelTask1",new RuntimeException("FAILED"))
                .expectTaskNotStarted("NextTask");

    }

    @Test
    void shouldExecuteTheNextTaskWhenParallelSubflowsComplete() {
        testFlow()
                .parallel(parallel -> parallel.subflow("Subflow1", subflow -> subflow.ioTask("parallelTask1")))
                .parallel(parallel -> parallel.subflow("Subflow2", subflow -> subflow.ioTask("parallelTask2")))
                .ioTask("NextTask")
                .build()
                .startFlow()
                .finishTask("parallelTask1")
                .expectTaskNotStarted("NextTask")
                .finishTask("parallelTask2")
                .assertTaskRunning("NextTask");
    }

}
