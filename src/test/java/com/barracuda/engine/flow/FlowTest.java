package com.barracuda.engine.flow;

import com.barracuda.engine.task.Task;
import lombok.Getter;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

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

        assertThat(ioTask.taskThread).isEqualTo(TaskCapturingThread.TaskThread.VIRTUAL);
        assertThat(cpuTask.taskThread).isEqualTo(TaskCapturingThread.TaskThread.PLATFORM);
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

        Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(flow::state, state -> assertThat(state).isEqualTo(FlowState.RUNNING));
    }

    @Test
    void shouldHaveCompletedStateOnceFinished() {
        var task = new BlockingTask();
        var flow = rootFlowBuilder.ioTask(task).build();

        ioTaskExecutor.submit(flow::execute);
        task.finish();

        Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(flow::state, state -> assertThat(state).isEqualTo(FlowState.COMPLETED));
    }

    @Test
    void shouldThrowISEWhenTryingToExecuteAlreadyRunningFlow() {
        var task = new BlockingTask();
        var flow = rootFlowBuilder.ioTask(task).build();

        ioTaskExecutor.submit(flow::execute);
        Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(flow::state, state -> assertThat(state).isEqualTo(FlowState.RUNNING));

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
                                .subflow( subflow -> subflow.ioTask(new ParallelTask(readinessLatch,barrierLatch)))
                                .subflow( subflow -> subflow.ioTask(new ParallelTask(readinessLatch,barrierLatch)))
                                .subflow( subflow -> subflow.ioTask(new ParallelTask(readinessLatch,barrierLatch)))
                ).build();

        ioTaskExecutor.submit(flow::execute);

        Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(readinessLatch::await);

        barrierLatch.countDown();

        Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(flow::state, state -> assertThat(state).isEqualTo(FlowState.COMPLETED));
    }

    @Test
    void shouldHavePausedStateWhenInterrupted() {
        var flow = rootFlowBuilder
                .ioTask(new BlockingTask())
                .parallel(parallel -> parallel.subflow(subflow -> subflow.ioTask(new BlockingTask())))
                .build();

        var flowTask = ioTaskExecutor.submit(flow::execute);

        Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(flow::state,state -> assertThat(state).isEqualTo(FlowState.RUNNING));

        flowTask.cancel(true);

        Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(flow::state,state -> assertThat(state).isEqualTo(FlowState.PAUSED));
    }

    @Test
    void shouldCancelAllRunningTaskWhenPaused() {
        var task = new BlockingTask();

        var flow = rootFlowBuilder
                .ioTask(task)
                .build();

        var flowTask = ioTaskExecutor.submit(flow::execute);

        Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(flow::state,state -> assertThat(state).isEqualTo(FlowState.RUNNING));
        Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(task::state,state -> assertThat(state).isEqualTo(BlockingTask.TaskState.WAITING));

        flowTask.cancel(true);

        Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(flow::state,state -> assertThat(state).isEqualTo(FlowState.PAUSED));

        Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(task::state,state -> assertThat(state).isEqualTo(BlockingTask.TaskState.INTERRUPTED));
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

        Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(flow::state,state -> assertThat(state).isEqualTo(FlowState.RUNNING));

        flowTask.cancel(true);
        Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(flow::state,state -> assertThat(state).isEqualTo(FlowState.PAUSED));

        assertThat(secondTask.state()).isEqualTo(BlockingTask.TaskState.CREATED);
    }

    @Disabled("TODO")
    @Test
    void shouldHaveFailedStateIfParallelSubflowFailsWithException(){

    }

    //create wait methods to make test code more readable

    private record ParallelTask(CountDownLatch notifyReadyLatch, CountDownLatch barrierLatch) implements Task<Void, Void> {

        @Override
            public Void execute(Void input) {
                notifyReadyLatch.countDown();
                try {
                    barrierLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return null;
            }
        }

    private static class TaskCapturingThread implements Task<Void,Void>{

        private enum TaskThread{
            VIRTUAL,PLATFORM,NONE
        }

        @Getter
        private volatile TaskThread taskThread = TaskThread.NONE;

        @Override
        public Void execute(Void input) {
            if (Thread.currentThread().isVirtual()) {
                taskThread = TaskThread.VIRTUAL;
            }else{
                taskThread = TaskThread.PLATFORM;
            }
            return null;
        }

    }

    private static class BlockingTask implements Task<Void, Void> {

        private enum TaskState {
            CREATED,WAITING,COMPLETED,INTERRUPTED;
        }
        private final AtomicReference<TaskState> state = new AtomicReference<>(TaskState.CREATED);
        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public Void execute(Void input) {
            state.set(TaskState.WAITING);
            try {
                latch.await();
            } catch (InterruptedException e) {
                state.set(TaskState.INTERRUPTED);
                throw new RuntimeException(e);
            }
            state.set(TaskState.COMPLETED);
            return null;
        }

        public void finish(){
            latch.countDown();
        }

        public TaskState state(){
            return state.get();
        }
    }

}
