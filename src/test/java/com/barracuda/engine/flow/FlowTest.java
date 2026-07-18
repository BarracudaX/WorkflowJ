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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.*;

@ExtendWith(OutputCaptureExtension.class)
public class FlowTest {

    private final ExecutorService cpuExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private final ExecutorService virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final RootFlowBuilder rootFlowBuilder = new RootFlowBuilder(cpuExecutor,virtualThreadExecutor);

    @Test
    void shouldSpecifyTasksThatExecute(CapturedOutput output) {
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

}
