package com.barracuda.engine.flow;

import com.barracuda.engine.builder.RootFlowBuilder;
import com.barracuda.engine.event.EvenPublisherImpl;
import com.barracuda.engine.event.FlowEvent.*;
import com.barracuda.engine.event.FlowEventPublisher;
import com.barracuda.engine.event.InMemoryEventCapturer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.barracuda.engine.flow.TestUtils.*;
import static org.assertj.core.api.Assertions.*;

@ExtendWith(OutputCaptureExtension.class)
public class FlowTest {

    private final ExecutorService cpuTaskExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private final ExecutorService ioTaskExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final InMemoryEventCapturer eventCapturer = new InMemoryEventCapturer();
    private final FlowEventPublisher eventPublisher = new EvenPublisherImpl();
    private final RootFlowBuilder rootFlowBuilder = new RootFlowBuilder(cpuTaskExecutor, ioTaskExecutor, eventPublisher).withID(1L);

    @BeforeEach
    void setUp() {
        eventPublisher.subscribe(eventCapturer);
    }

    @Test
    void shouldExecuteTasksInSpecifiedOrder(CapturedOutput output) {
        Flow flow = rootFlowBuilder
                .runnableTask(() -> System.out.println("1"), 1L)
                .runnableTask(() -> System.out.println("2"), 2L)
                .runnableTask(() -> System.out.println("3"), 3L)
                .build();

        ioTaskExecutor.submit(flow::execute);
        waitUntilCompleted(flow);

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
        TaskCapturingThread ioTask = new TaskCapturingThread(1L);
        TaskCapturingThread cpuTask = new TaskCapturingThread(2L);

        //root flow builder is configured with virtual executor for io tasks and fixed executor for cpu tasks.
        Flow flow = rootFlowBuilder
                .ioTask(ioTask)
                .cpuTask(cpuTask)
                .build();

        flow.execute();

        assertThat(ioTask.getTaskThread()).isEqualTo(TaskCapturingThread.TaskThread.VIRTUAL);
        assertThat(cpuTask.getTaskThread()).isEqualTo(TaskCapturingThread.TaskThread.PLATFORM);
    }

    @Test
    void newlyCreatedFlowShouldBeInCreatedState() {
        Flow flow = rootFlowBuilder.build();

        assertThat(flow.state()).isEqualTo(FlowState.CREATED);
    }

    @Test
    void runningFlowShouldHaveRunningState() {
        var flow = rootFlowBuilder.ioTask(new TestTask(1L)).build();

        ioTaskExecutor.submit(flow::execute);

        waitUntilRunning(flow);
    }

    @Test
    void shouldHaveCompletedStateOnceFinished() {
        var task = new TestTask(1L);
        var flow = rootFlowBuilder.ioTask(task).build();

        ioTaskExecutor.submit(flow::execute);
        waitUntilRunning(task);
        task.finish();

        waitUntilCompleted(flow);
    }

    @Test
    void shouldThrowISEWhenTryingToExecuteAlreadyRunningFlow() {
        var task = new TestTask(1L);
        var flow = rootFlowBuilder.ioTask(task).build();

        ioTaskExecutor.submit(flow::execute);
        waitUntilRunning(flow);

        Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(() -> assertThatThrownBy(flow::execute).isInstanceOf(IllegalStateException.class));
    }

    @Test
    void shouldHaveFailedStateIfTaskFailsWithException() {
        var exception = new RuntimeException("FAILED");
        var failTask = new TestTask(1L);
        var flow = rootFlowBuilder.ioTask(failTask).build();

        var flowResult = ioTaskExecutor.submit(flow::execute);
        waitUntilRunning(failTask);
        failTask.failNow(exception);

        waitUntilFailed(flow);
        assertThatThrownBy(flowResult::get).hasCause(exception);
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

        waitUntilCompleted(flow);
    }

    @Test
    void shouldHavePausedStateWhenInterrupted() {
        TestTask task = new TestTask(1L);
        var flow = rootFlowBuilder
                .ioTask(task)
                .build();

        var flowTask = ioTaskExecutor.submit(flow::execute);

        waitUntilRunning(flow);

        flowTask.cancel(true);

        waitUntilPaused(flow);
    }

    @Test
    void shouldCancelAllRunningTaskWhenPaused() {
        var task = new TestTask(1L);

        var flow = rootFlowBuilder
                .ioTask(task)
                .build();

        var flowTask = ioTaskExecutor.submit(flow::execute);

        waitUntilRunning(flow);
        waitUntilRunning(task);

        flowTask.cancel(true);

        waitUntilPaused(flow);

        waitUntilInterrupted(task);
    }

    @Test
    void shouldNotExecuteNextTaskWhenInterrupted() {
        var firstTask = new TestTask(1L);
        var secondTask = new TestTask(2L);

        var flow = rootFlowBuilder
                .ioTask(firstTask)
                .ioTask(secondTask)
                .build();

        var flowTask = ioTaskExecutor.submit(flow::execute);

        waitUntilRunning(flow);
        waitUntilRunning(firstTask);

        flowTask.cancel(true);
        waitUntilPaused(flow);

        assertThat(secondTask.state()).isEqualTo(TestTaskState.CREATED);
    }

    @Test
    void shouldHaveFailedStateIfParallelSubflowFailsWithException() {
        RuntimeException exception = new RuntimeException("I fail,lol");
        var failTask = new TestTask(1L);
        var parallelTask2 = new TestTask(2L);
        var parallelTask3 = new TestTask(3L);

        var flow = rootFlowBuilder
                .parallel(parallel ->
                        parallel
                                .subflow(subflow -> subflow.ioTask(failTask).withID(1L))
                                .subflow(subflow -> subflow.ioTask(parallelTask2).withID(2L))
                                .subflow(subflow -> subflow.ioTask(parallelTask3).withID(3L))
                ).build();
        var flowTask = ioTaskExecutor.submit(flow::execute);

        waitUntilRunning(flow);
        waitUntilRunning(parallelTask2);
        waitUntilRunning(parallelTask3);
        failTask.failNow(exception);

        waitUntilFailed(flow);
        assertThatCode(flowTask::get).hasCause(exception);
    }

    @Test
    void shouldCancelParallelSubflowsIfOneOfThemFails() {
        var failTask = new TestTask(1L);
        var parallelTask2 = new TestTask(2L);
        var parallelTask3 = new TestTask(3L);

        var flow = rootFlowBuilder
                .parallel(parallel ->
                        parallel
                                .subflow(subflow -> subflow.ioTask(failTask).withID(1L))
                                .subflow(subflow -> subflow.ioTask(parallelTask2).withID(2L))
                                .subflow(subflow -> subflow.ioTask(parallelTask3).withID(3L))
                ).build();

        ioTaskExecutor.submit(flow::execute);

        waitUntilRunning(flow);
        waitUntilRunning(failTask);
        waitUntilRunning(parallelTask2);
        waitUntilRunning(parallelTask3);

        failTask.failNow(new RuntimeException("FAIL"));

        waitUntilInterrupted(parallelTask2);
        waitUntilInterrupted(parallelTask3);
    }

    @Test
    void shouldNotRunNextTaskWhenParallelSubflowFails() {
        var failTask = new TestTask(1L);
        var parallelTask2 = new TestTask(2L);
        var nextTask = new TestTask(3L);

        var flow = rootFlowBuilder
                .parallel(parallel ->
                        parallel
                                .subflow(subflow -> subflow.ioTask(failTask).withID(1L))
                                .subflow(subflow -> subflow.ioTask(parallelTask2).withID(2L))
                ).ioTask(nextTask)
                .build();

        ioTaskExecutor.submit(flow::execute);
        waitUntilRunning(flow);
        failTask.failNow(new RuntimeException("FAIL"));
        waitUntilFailed(flow);

        assertThat(nextTask.state()).isEqualTo(TestTaskState.CREATED);
    }

    @Test
    void shouldExecuteTheNextTaskWhenParallelSubflowsComplete() {
        var parallelTask1 = new TestTask(1L);
        var parallelTask2 = new TestTask(2L);
        var nextTask = new TestTask(3L);

        var flow = rootFlowBuilder
                .parallel(parallel ->
                        parallel
                                .subflow(subflow -> subflow.ioTask(parallelTask1).withID(1L))
                                .subflow(subflow -> subflow.ioTask(parallelTask2).withID(2L))
                ).ioTask(nextTask)
                .build();
        ioTaskExecutor.submit(flow::execute);
        waitUntilRunning(flow);

        parallelTask1.finish();
        assertThat(nextTask.state()).isEqualTo(TestTaskState.CREATED);
        parallelTask2.finish();

        waitUntilRunning(nextTask);
    }

    @Test
    void shouldPauseFlowIfTaskFailsWithFlowInterruptedException() {
        var singleTaskFlow = createRunningFlowWithOneTask(rootFlowBuilder, ioTaskExecutor);

        singleTaskFlow.task().failNow(new FlowInterruptedException("Simulating interruption"));
        waitUntilPaused(singleTaskFlow.flow());
    }

    @Test
    void shouldAllowResumingPausedFlowByExecutingItAgain() {
        var task = new TestTask(1L);

        var flow = rootFlowBuilder.ioTask(task).build();
        var flowTask = ioTaskExecutor.submit(flow::execute);
        waitUntilRunning(flow);
        waitUntilRunning(task);

        flowTask.cancel(true);

        waitUntilPaused(flow);
        waitUntilInterrupted(task);

        ioTaskExecutor.submit(flow::execute);
        waitUntilRunning(flow);
        waitUntilRunning(task);
        task.finish();

        waitUntilCompleted(flow);
    }

    @Test
    void shouldPublishFlowStartedEventWhenStartingTheFlow() {
        createRunningFlowWithOneTask(rootFlowBuilder, ioTaskExecutor)
                .verifyWithFlowID(flowID -> assertThat(eventCapturer.events()).contains(new FlowStartedEvent(flowID)));
    }

    @Test
    void shouldPublishTaskStartedEventWhenExecutingTheTask() {
        createRunningFlowWithOneTask(rootFlowBuilder, ioTaskExecutor)
                .verifyWithTaskID(taskID -> assertThat(eventCapturer.events()).contains(new TaskStartedEvent(taskID)));
    }

    @Test
    void shouldPublishTaskCompletedEventWhenTaskFinishesNormally() {
        createRunningFlowWithOneTask(rootFlowBuilder, ioTaskExecutor)
                .finishTask()
                .waitUntilFlowCompleted()
                .verifyWithTaskID(taskID -> assertThat(eventCapturer.events()).contains(new TaskCompletedEvent(taskID)));
    }

    @Test
    void shouldPublishFlowCompletedEventWhenFlowFinishesNormally() {
        createRunningFlowWithOneTask(rootFlowBuilder, ioTaskExecutor)
                .finishTask()
                .waitUntilFlowCompleted()
                .verifyWithFlowID(flowID -> assertThat(eventCapturer.events()).contains(new FlowCompletedEvent(flowID)));
    }

    @Test
    void shouldPublishTaskFailedEventWhenTaskFinishesWithAnException() {
        var exception = new RuntimeException("FAILED");
        createRunningFlowWithOneTask(rootFlowBuilder, ioTaskExecutor)
                .failTask(exception)
                .waitUntilFlowFailed()
                .verifyWithTaskID(taskID -> assertThat(eventCapturer.events()).contains(new TaskFailedEvent(taskID, exception)));
    }

    @Test
    void shouldPublishFlowFailedEventWhenATaskFailsWithAnException() {
        var exception = new RuntimeException("FAILED");
        createRunningFlowWithOneTask(rootFlowBuilder, ioTaskExecutor)
                .failTask(exception)
                .waitUntilFlowFailed()
                .verifyWithFlowID(flowID -> assertThat(eventCapturer.events()).contains(new FlowFailedEvent(flowID, exception)));
    }

    @Test
    void shouldPublishFlowPausedEventWhenInterrupted() {
        createRunningFlowWithOneTask(rootFlowBuilder, ioTaskExecutor)
                .cancel()
                .waitUntilFlowPaused()
                .verifyWithFlowID(flowID -> assertThat(eventCapturer.events()).contains(new FlowPausedEvent(flowID)));
    }

    @Test
    void shouldPublishTaskPausedEvenWhenTaskInterrupted() {
        createRunningFlowWithOneTask(rootFlowBuilder, ioTaskExecutor)
                .cancel()
                .waitUntilFlowPaused()
                .verifyWithTaskID( taskID -> assertThat(eventCapturer.events()).contains(new TaskPausedEvent(taskID)));
    }

}
