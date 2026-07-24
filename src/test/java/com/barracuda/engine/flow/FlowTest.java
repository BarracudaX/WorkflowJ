package com.barracuda.engine.flow;

import com.barracuda.engine.event.ExecutionEvent.CommandEvent.Continue;
import com.barracuda.engine.test.flow.TestFlow;
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
        Flow flow = flowBuilder
                .runnableTask(() -> System.out.println("1"), 1L)
                .runnableTask(() -> System.out.println("2"), 2L)
                .runnableTask(() -> System.out.println("3"), 3L)
                .build();

        ioTaskExecutor.submit(() -> flow.event(new Continue()));

        AwaitilityUtils.waitUntilFlowCompleted(flow, Duration.ofSeconds(1));

        assertThat(output.getAll().lines().toList()).containsExactly("1", "2", "3");
    }

    @Test
    void shouldAllowCreationOfEmptyFlow() {
        assertThatCode(flowBuilder::build).doesNotThrowAnyException();
    }

    @Test
    void shouldExecuteIoAndCpuTasksOnDifferentExecutors() {
        //Note that testFlow by default runs IO tasks on virtual thread and cpu tasks on platform threads.
        testFlow()
                .ioTask("IoTask")
                .cpuTask("CpuTask")
                .build()
                .startFlow()
                .finishTask("IoTask")
                .finishTask("CpuTask")
                .expectTaskRanOnVirtualThread("IoTask")
                .expectTaskRanOnPlatformThread("CpuTask");
    }

    @Disabled("need to figure out how to assert sequentiality")
    @Test
    void shouldExecutedTasksSequentially() {
    }

    @Disabled("Already tested by FlowEventReplayingTest.shouldNotAllowSendingEventsToFlowThatIsRunning")
    @Test
    void shouldThrowISEWhenTryingToExecuteAlreadyRunningFlow() {
        testFlow()
                .ioTask("task")
                .build()
                .startFlow()
                .assertThrows(TestFlow::startSync, error -> error.isInstanceOf(IllegalStateException.class));
    }
}
