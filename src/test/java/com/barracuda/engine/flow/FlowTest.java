package com.barracuda.engine.flow;

import com.barracuda.engine.task.Task;
import lombok.Getter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.*;

@ExtendWith(OutputCaptureExtension.class)
public class FlowTest {

    private final ExecutorService cpuExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private final ExecutorService virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final RootFlowBuilder rootFlowBuilder = new RootFlowBuilder(cpuExecutor,virtualThreadExecutor);

    @Test
    void shouldSpecifyTasksThatExecuteSequentially(CapturedOutput output) {
        Flow flow = rootFlowBuilder
                .runnableTask(() -> System.out.println("1"))
                .runnableTask(() -> System.out.println("2"))
                .runnableTask(() -> System.out.println("3"))
                .build();

        flow.execute();

        assertThat(output.getAll().lines().toList()).containsExactly("1", "2", "3");
    }

    @Test
    void shouldAllowCreationOfEmptyFlow() {
        assertThatCode(rootFlowBuilder::build).doesNotThrowAnyException();
    }

    @Test
    void shouldExecuteIOTasksOnVirtualThreadAndCpuTasksOnPlatformThreads() {
        TaskCapturingThread ioTask = new TaskCapturingThread();
        TaskCapturingThread cpuTask = new TaskCapturingThread();
        Flow flow = rootFlowBuilder
                .ioTask(ioTask)
                .cpuTask(cpuTask)
                .build();

        flow.execute();

        assertThat(ioTask.taskThread).isEqualTo(TaskThread.VIRTUAL);
        assertThat(cpuTask.taskThread).isEqualTo(TaskThread.PLATFORM);
    }

    private enum TaskThread{
        VIRTUAL,PLATFORM,NONE
    }

    private class TaskCapturingThread implements Task<Void,Void>{

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
