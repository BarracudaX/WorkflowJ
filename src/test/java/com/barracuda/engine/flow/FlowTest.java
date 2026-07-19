package com.barracuda.engine.flow;

import com.barracuda.engine.builder.RootFlowBuilder;
import com.barracuda.engine.task.Task;
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
import static com.barracuda.engine.flow.TestUtils.BlockingTask.*;
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

        Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(flow::execute);

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
        var flow = rootFlowBuilder.ioTask(new BlockingTask()).build();

        ioTaskExecutor.submit(flow::execute);

    waitUntilRunning(flow);
    }

    @Test
    void shouldHaveCompletedStateOnceFinished() {
        var task = new BlockingTask();
        var flow = rootFlowBuilder.ioTask(task).build();

        ioTaskExecutor.submit(flow::execute);
        task.finish();

        waitUntilCompleted(flow);
    }

    @Test
    void shouldThrowISEWhenTryingToExecuteAlreadyRunningFlow() {
        var task = new BlockingTask();
        var flow = rootFlowBuilder.ioTask(task).build();

        ioTaskExecutor.submit(flow::execute);
        waitUntilRunning(flow);

        Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(() -> assertThatThrownBy(flow::execute).isInstanceOf(IllegalStateException.class));
    }

    @Test
    void shouldHaveFailedStateIfTaskFailsWithException() {
        RuntimeException exception = new RuntimeException("FAILED");

        var flow = rootFlowBuilder.ioTask(Task.fromRunnable(() -> { throw exception; })).build();

        assertThatThrownBy(flow::execute).isEqualTo(exception);
        assertThat(flow.state()).isEqualTo(FlowState.FAILED);
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
        var flow = rootFlowBuilder
                .ioTask(new BlockingTask())
                .parallel(parallel -> parallel.subflow(subflow -> subflow.ioTask(new BlockingTask())))
                .build();

        var flowTask = ioTaskExecutor.submit(flow::execute);

        waitUntilRunning(flow);

        flowTask.cancel(true);

        waitUntilPaused(flow);
    }

    @Test
    void shouldCancelAllRunningTaskWhenPaused() {
        var task = new BlockingTask();

        var flow = rootFlowBuilder
                .ioTask(task)
                .build();

        var flowTask = ioTaskExecutor.submit(flow::execute);

        waitUntilRunning(flow);
        task.waitUntilRunning();

        flowTask.cancel(true);

        waitUntilPaused(flow);

        task.waitUntilInterrupted();
    }

    @Test
    void shouldNotExecuteNextTaskWhenInterrupted() {
        var firstTask = new BlockingTask();
        var secondTask = new BlockingTask();

        var flow = rootFlowBuilder
                .ioTask(firstTask)
                .ioTask(secondTask)
                .build();

        var flowTask = ioTaskExecutor.submit(flow::execute);

        waitUntilRunning(flow);

        flowTask.cancel(true);
        waitUntilPaused(flow);

        assertThat(secondTask.state()).isEqualTo(TaskState.CREATED);
    }

    @Test
    void shouldHaveFailedStateIfParallelSubflowFailsWithException(){
        RuntimeException exception = new RuntimeException("I fail,lol");
        var failTask = new FailOnDemandTask(exception);
        var parallelTask2 = new BlockingTask();
        var parallelTask3 = new BlockingTask();

        var flow = rootFlowBuilder
                .parallel(parallel ->
                        parallel
                                .subflow( subflow -> subflow.ioTask(failTask))
                                .subflow( subflow -> subflow.ioTask(parallelTask2))
                                .subflow( subflow -> subflow.ioTask(parallelTask3))
                ).build();
        var flowTask = ioTaskExecutor.submit(flow::execute);

        waitUntilRunning(flow);
        parallelTask2.waitUntilRunning();
        parallelTask3.waitUntilRunning();
        failTask.failNow();

        assertThatCode(flowTask::get).hasCause(exception);
        waitUntilFailed(flow);
    }

    @Test
    void shouldCancelParallelSubflowsIfOneOfThemFails() {
        var failTask = new FailOnDemandTask(new RuntimeException("I fail,lol"));
        var parallelTask2 = new BlockingTask();
        var parallelTask3 = new BlockingTask();

        var flow = rootFlowBuilder
                .parallel(parallel ->
                        parallel
                                .subflow( subflow -> subflow.ioTask(failTask))
                                .subflow( subflow -> subflow.ioTask(parallelTask2))
                                .subflow( subflow -> subflow.ioTask(parallelTask3))
                ).build();
        ioTaskExecutor.submit(flow::execute);

        waitUntilRunning(flow);
        parallelTask2.waitUntilRunning();
        parallelTask3.waitUntilRunning();

        failTask.failNow();

        parallelTask2.waitUntilInterrupted();
        parallelTask3.waitUntilInterrupted();
    }

    @Test
    void shouldStopFlowExecutionWhenParallelSubflowFails() {
        var failTask = new FailOnDemandTask(new RuntimeException("I fail,lol"));
        var parallelTask2 = new BlockingTask();
        var nextTask = new BlockingTask();

        var flow = rootFlowBuilder
                .parallel(parallel ->
                        parallel
                                .subflow( subflow -> subflow.ioTask(failTask))
                                .subflow( subflow -> subflow.ioTask(parallelTask2))
                ).ioTask(nextTask)
                .build();

        ioTaskExecutor.submit(flow::execute);
        waitUntilRunning(flow);
        failTask.failNow();
        waitUntilFailed(flow);

        assertThat(nextTask.state()).isEqualTo(TaskState.CREATED);
    }

    @Test
    void shouldExecuteTheNextTaskWhenParallelSubflowsComplete() {
        var parallelTask1 = new BlockingTask();
        var parallelTask2 = new BlockingTask();
        var nextTask = new BlockingTask();

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
        assertThat(nextTask.state()).isEqualTo(TaskState.CREATED);

        parallelTask2.finish();

        nextTask.waitUntilRunning();
    }
}
