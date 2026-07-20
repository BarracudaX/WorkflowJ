package com.barracuda.engine.flow;

import com.barracuda.engine.utility.AwaitilityUtils;
import com.barracuda.engine.builder.RootFlowBuilder;
import com.barracuda.engine.test.ParallelTestTask;
import com.barracuda.engine.test.TestTaskVerifier;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.barracuda.engine.test.TestFlowBuilder.testFlow;
import static org.assertj.core.api.Assertions.*;


@ExtendWith(OutputCaptureExtension.class)
public class FlowTest {

    private final ExecutorService cpuTaskExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private final ExecutorService ioTaskExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final RootFlowBuilder rootFlowBuilder = new RootFlowBuilder(cpuTaskExecutor, ioTaskExecutor).withID(1L);


    @Test
    void shouldExecuteTasksInSpecifiedOrder(CapturedOutput output) {
        Flow flow = rootFlowBuilder
                .runnableTask(() -> System.out.println("1"), 1L)
                .runnableTask(() -> System.out.println("2"), 2L)
                .runnableTask(() -> System.out.println("3"), 3L)
                .build();

        ioTaskExecutor.submit(flow::execute);
        AwaitilityUtils.waitUntilFlowCompleted(flow);

        assertThat(output.getAll().lines().toList()).containsExactly("1", "2", "3");
    }

    @Disabled("need to figure out how to assert sequentiality")
    @Test
    void shouldExecutedTasksSequentially() {
    }

    @Test
    void shouldAllowCreationOfEmptyFlow() {
        assertThatCode(rootFlowBuilder::build).doesNotThrowAnyException();
    }

    @Test
    void shouldExecuteIoAndCpuTasksOnDifferentExecutors() {

        //Note that testFlow by default runs IO tasks on virtual thread and cpu tasks on platform threads.
        testFlow()
                .task("IoTask")
                .cpuTask("CpuTask")
                .build()
                .startFlow()
                .finishTask("IoTask")
                .finishTask("CpuTask")
                .assertTaskRanOnVirtualThread("IoTask")
                .assertTaskRanOnPlatformThread("CpuTask");
    }

    @Test
    void newlyCreatedFlowShouldBeInCreatedState() {
        testFlow()
                .task("Task")
                .build()
                .expectFlowInCreatedState();
    }

    @Test
    void runningFlowShouldHaveRunningState() {
        testFlow()
                .task("Task")
                .build()
                .startFlow()
                .expectIsRunning();
    }

    @Test
    void shouldHaveCompletedStateOnceFinished() {
        testFlow()
                .task("Task")
                .build()
                .startFlow()
                .finishTask("Task")
                .expectFlowCompleted();
    }

    @Disabled("TODO")
    @Test
    void shouldThrowISEWhenTryingToExecuteAlreadyRunningFlow() {
    }

    @Test
    void shouldHaveFailedStateIfTaskFailsWithException() {
        var exception = new RuntimeException("FAILED");
        testFlow()
                .task("FailTask")
                .build()
                .startFlow()
                .failTask("FailTask", exception)
                .expectFlowFailed(exception);
    }

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
    void shouldHavePausedStateWhenInterrupted() {
        testFlow()
                .task("FirstTask")
                .build()
                .startFlow()
                .interruptFlowAndExpectFlowPaused();
    }

    @Test
    void shouldCancelAllRunningTaskWhenPaused() {
        testFlow()
                .task("FirstTask")
                .build()
                .startFlow()
                .interruptFlowAndExpectFlowPaused()
                .assertTaskCancelled("FirstTask");
    }

    @Test
    void shouldNotExecuteNextTaskWhenInterrupted() {
        testFlow()
                .task("FirstTask")
                .task("SecondTask")
                .build()
                .startFlow()
                .assertTaskRunning("FirstTask")
                .interruptFlowAndExpectFlowPaused()
                .assertTaskCancelled("FirstTask")
                .assertTaskNotStarted("SecondTask");
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

    @Disabled("Need to figure out the best way to interrupt the task")
    @Test
    void shouldPauseFlowIfTaskInterrupted() {
    }

    @Disabled("Not yet sure how resumption will be implemented.")
    @Test
    void shouldAllowResumingPausedFlowByExecutingItAgain() {

    }

    @Test
    void shouldPublishFlowStartedEventWhenStartingTheFlow() {
        testFlow()
                .task("test")
                .build()
                .startFlow()
                .assertFlowEventsInOrder(events -> events.hasFlowStartedEvent().andHasNoMoreEvents());
    }

    @Test
    void shouldPublishTaskStartedEventWhenExecutingTheTask() {
        testFlow()
                .task("test")
                .build()
                .startFlow()
                .assertTaskEventsInOrder("test",events -> events.hasTaskStartedEvent().andHasNoMoreEvents());
    }

    @Test
    void shouldPublishTaskCompletedEventWhenTaskFinishesNormally() {
        testFlow()
                .task("test")
                .build()
                .startFlow()
                .finishTask("test")
                .assertTaskEventsInOrder("test",events -> events.hasTaskStartedEvent().hasTaskCompletedEvent().andHasNoMoreEvents());
    }

    @Test
    void shouldPublishFlowCompletedEventWhenFlowFinishesNormally() {
        testFlow()
                .task("test")
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
                .task("test")
                .build()
                .startFlow()
                .failTask("test", exception)
                .expectFlowFailed()
                .assertFlowEventsInOrder( events -> events.hasFlowStartedEvent().hasFlowFailedEvent(exception).andHasNoMoreEvents());

    }

    @Test
    void shouldPublishTaskFailedEventWhenTaskFinishesWithAnException() {
        var exception = new RuntimeException("FAILED");

        testFlow()
                .task("test")
                .build()
                .startFlow()
                .failTask("test", exception)
                .expectFlowFailed()
                .assertTaskEventsInOrder("test",events -> events.hasTaskStartedEvent().hasTaskFailedEvent(exception).andHasNoMoreEvents());
    }

    @Test
    void shouldPublishFlowPausedEventWhenInterrupted() {
        testFlow()
                .task("test")
                .build()
                .startFlow()
                .interruptFlowAndExpectFlowPaused()
                .assertFlowEventsInOrder(events -> events.hasFlowStartedEvent().hasFlowPausedEvent().andHasNoMoreEvents());
    }

    @Test
    void shouldPublishTaskPausedEventWhenTaskInterrupted() {
        testFlow()
                .task("test")
                .build()
                .startFlow()
                .interruptFlowAndExpectFlowPaused()
                .assertTaskEventsInOrder("test", events -> events.hasTaskStartedEvent().hasTaskPausedEvent().andHasNoMoreEvents());
    }

}
