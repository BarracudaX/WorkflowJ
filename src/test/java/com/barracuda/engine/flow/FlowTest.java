package com.barracuda.engine.flow;

import com.barracuda.engine.event.ExecutionEvent.ContinueEvent;
import com.barracuda.engine.utility.AwaitilityUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import java.time.Duration;

import static com.barracuda.engine.test.builder.TestFlowBuilder.testFlow;
import static org.assertj.core.api.Assertions.*;


@ExtendWith(OutputCaptureExtension.class)
public class FlowTest extends AbstractFlowTest{


    @Test
    void shouldExecuteTasksInSpecifiedOrder(CapturedOutput output) {
        Flow flow = rootFlowBuilder
                .runnableTask(() -> System.out.println("1"), 1L)
                .runnableTask(() -> System.out.println("2"), 2L)
                .runnableTask(() -> System.out.println("3"), 3L)
                .build();

        ioTaskExecutor.submit(() -> flow.event(new ContinueEvent(flow.id())));

        AwaitilityUtils.waitUntilFlowCompleted(flow, Duration.ofSeconds(1));

        assertThat(output.getAll().lines().toList()).containsExactly("1", "2", "3");
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

    @Disabled("Not yet sure how resumption will be implemented.")
    @Test
    void shouldAllowResumingPausedFlowByExecutingItAgain() {

    }

    @Disabled("need to figure out how to assert sequentiality")
    @Test
    void shouldExecutedTasksSequentially() {
    }

    @Disabled("TODO")
    @Test
    void shouldThrowISEWhenTryingToExecuteAlreadyRunningFlow() {
    }
}
