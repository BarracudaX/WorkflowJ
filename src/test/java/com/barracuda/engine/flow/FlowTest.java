package com.barracuda.engine.flow;

import com.barracuda.engine.builder.RootFlowBuilder;
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

import static com.barracuda.engine.flow.TestUtils.*;
import static org.assertj.core.api.Assertions.*;

@ExtendWith(OutputCaptureExtension.class)
public class FlowTest {

    private final ExecutorService cpuTaskExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private final ExecutorService ioTaskExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final RootFlowBuilder rootFlowBuilder = new RootFlowBuilder(cpuTaskExecutor, ioTaskExecutor);

    @Test
    void shouldExecuteTasksInSpecifiedOrder(CapturedOutput output) {
        Flow flow = rootFlowBuilder
                .runnableTask(() -> System.out.println("1"))
                .runnableTask(() -> System.out.println("2"))
                .runnableTask(() -> System.out.println("3"))
                .build();

        ioTaskExecutor.submit(flow::execute);
        waitUntilCompleted(flow);

        assertThat(output.getAll().lines().toList()).containsExactly("1", "2", "3");
    }

    @Disabled("need to figure out how to assert sequentiality")
    @Test
    void shouldExecutedTasksSequentially() { }

    @Test
    void shouldAllowCreationOfEmptyFlow() {
        assertThatCode(rootFlowBuilder::build).doesNotThrowAnyException();
    }

    @Test
    void shouldExecuteIoAndCpuTasksOnDifferentExecutors() {
        TaskCapturingThread ioTask = new TaskCapturingThread();
        TaskCapturingThread cpuTask = new TaskCapturingThread();

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
        var flow = rootFlowBuilder.ioTask(new TestTask()).build();

        ioTaskExecutor.submit(flow::execute);

        waitUntilRunning(flow);
    }

    @Test
    void shouldHaveCompletedStateOnceFinished() {
        var task = new TestTask();
        var flow = rootFlowBuilder.ioTask(task).build();

        ioTaskExecutor.submit(flow::execute);
        waitUntilRunning(task);
        task.finish();

        waitUntilCompleted(flow);
    }

    @Test
    void shouldThrowISEWhenTryingToExecuteAlreadyRunningFlow() {
        var task = new TestTask();
        var flow = rootFlowBuilder.ioTask(task).build();

        ioTaskExecutor.submit(flow::execute);
        waitUntilRunning(flow);

        Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(() -> assertThatThrownBy(flow::execute).isInstanceOf(IllegalStateException.class));
    }

    @Test
    void shouldHaveFailedStateIfTaskFailsWithException() {
        var exception = new RuntimeException("FAILED");
        var failTask = new TestTask();
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
                                .subflow( subflow -> subflow.ioTask(new ParallelTestTask(readinessLatch,barrierLatch)))
                                .subflow( subflow -> subflow.ioTask(new ParallelTestTask(readinessLatch,barrierLatch)))
                                .subflow( subflow -> subflow.ioTask(new ParallelTestTask(readinessLatch,barrierLatch)))
                ).build();

        ioTaskExecutor.submit(flow::execute);

        Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(readinessLatch::await);

        barrierLatch.countDown();

        waitUntilCompleted(flow);
    }

    @Test
    void shouldHavePausedStateWhenInterrupted() {
        TestTask task = new TestTask();
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
        var task = new TestTask();

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
        var firstTask = new TestTask();
        var secondTask = new TestTask();

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
    void shouldHaveFailedStateIfParallelSubflowFailsWithException(){
        RuntimeException exception = new RuntimeException("I fail,lol");
        var failTask = new TestTask();
        var parallelTask2 = new TestTask();
        var parallelTask3 = new TestTask();

        var flow = rootFlowBuilder
                .parallel(parallel ->
                        parallel
                                .subflow( subflow -> subflow.ioTask(failTask))
                                .subflow( subflow -> subflow.ioTask(parallelTask2))
                                .subflow( subflow -> subflow.ioTask(parallelTask3))
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
        var failTask = new TestTask();
        var parallelTask2 = new TestTask();
        var parallelTask3 = new TestTask();

        var flow = rootFlowBuilder
                .parallel(parallel ->
                        parallel
                                .subflow( subflow -> subflow.ioTask(failTask))
                                .subflow( subflow -> subflow.ioTask(parallelTask2))
                                .subflow( subflow -> subflow.ioTask(parallelTask3))
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
        var failTask = new TestTask();
        var parallelTask2 = new TestTask();
        var nextTask = new TestTask();

        var flow = rootFlowBuilder
                .parallel(parallel ->
                        parallel
                                .subflow( subflow -> subflow.ioTask(failTask))
                                .subflow( subflow -> subflow.ioTask(parallelTask2))
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
        var parallelTask1 = new TestTask();
        var parallelTask2 = new TestTask();
        var nextTask = new TestTask();

        var flow = rootFlowBuilder
                .parallel(parallel ->
                        parallel
                                .subflow( subflow -> subflow.ioTask(parallelTask1))
                                .subflow( subflow -> subflow.ioTask(parallelTask2))
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
        var task = new TestTask();

        var flow  = rootFlowBuilder.ioTask(task).build();

        ioTaskExecutor.submit(flow::execute);
        waitUntilRunning(flow);
        waitUntilRunning(task);

        task.failNow(new FlowInterruptedException("Simulating interruption"));
        waitUntilPaused(flow);
    }

    @Test
    void shouldAllowResumingPausedFlowByExecutingItAgain() {
        var task = new TestTask();

        var flow  = rootFlowBuilder.ioTask(task).build();
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
}
