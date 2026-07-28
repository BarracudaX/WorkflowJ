package com.barracuda.engine.flow;

import com.barracuda.engine.command.Command.Continue;
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
    void shouldAllowExecutingTasksInParallelWithSubflows() {
        var readinessLatch = new CountDownLatch(3);
        var barrierLatch = new CountDownLatch(1);

        var flow = flowBuilder
                .parallel(parallel ->
                        parallel
                                .subflow(2L,subflow -> subflow.actionTask(new ParallelTestTask(readinessLatch, barrierLatch, 1L)))
                                .subflow(3L,subflow -> subflow.actionTask(new ParallelTestTask(readinessLatch, barrierLatch, 2L)))
                                .subflow(4L,subflow -> subflow.actionTask(new ParallelTestTask(readinessLatch, barrierLatch, 3L)))
                ).build();

        ioTaskExecutor.submit(() -> flow.command(new Continue()));

        Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(readinessLatch::await); // wait for all 3 tasks to start

        barrierLatch.countDown(); // let the tasks finish

        AwaitilityUtils.waitUntilFlowCompleted(flow, Duration.ofSeconds(1));
    }

    @Test
    void flowShouldHaveFailedStateIfParallelSubflowFailsWithException() {
        var exception = new RuntimeException("FAILED");
        TestFlow testFlow = testFlow()
                .parallel(parallel -> parallel
                        .subflow("Subflow1", subflow -> subflow.actionTask("ParallelFailTask"))
                        .subflow("Subflow2", subflow -> subflow.actionTask("ParallelTask2"))
                        .subflow("Subflow3", subflow -> subflow.actionTask("ParallelTask3"))
                )
                .build()
                .startFlow()
                .failTask("ParallelFailTask", exception);

        assertThat(testFlow).hasEventuallyFailedWith(exception);
    }

    @Test
    void shouldCancelParallelTasksOfASubflowIfOneOfThemFails() {
        TestFlow testFlow = testFlow()
                .parallel(parallel -> parallel
                        .subflow("Subflow1", subflow -> subflow.actionTask("parallelTask1"))
                        .subflow("Subflow2", subflow -> subflow.actionTask("parallelTask2"))
                        .subflow("Subflow3", subflow -> subflow.actionTask("parallelTask3"))
                )
                .build()
                .startFlow()
                .failTask("parallelTask1", new RuntimeException("FAILED"));

        assertThat(testFlow).hasTaskSatisfying("parallelTask2", TestTaskAssert::isEventuallyCancelled);
        assertThat(testFlow).hasTaskSatisfying("parallelTask3", TestTaskAssert::isEventuallyCancelled);
    }

    @Test
    void shouldNotRunNextTaskWhenParallelSubflowFails() {
        TestFlow testFlow = testFlow()
                .parallel(parallel -> parallel.subflow("Subflow1", subflow -> subflow.actionTask("parallelTask1")))
                .actionTask("NextTask")
                .build()
                .startFlow()
                .failTask("parallelTask1", new RuntimeException("FAILED"));

        assertThat(testFlow).hasTaskSatisfying("NextTask", TestTaskAssert::hasNotStarted);
    }

    @Test
    void shouldExecuteTheNextTaskWhenParallelSubflowsComplete() {
        TestFlow testFlow = testFlow()
                .parallel(parallel -> parallel.subflow("Subflow1", subflow -> subflow.actionTask("parallelTask1")))
                .parallel(parallel -> parallel.subflow("Subflow2", subflow -> subflow.actionTask("parallelTask2")))
                .actionTask("NextTask")
                .build()
                .startFlow()
                .finishTask("parallelTask1")
                .finishTask("parallelTask2");

        assertThat(testFlow).hasTaskSatisfying("NextTask", TestTaskAssert::isRunning);

    }

}
