package com.barracuda.engine.workflow;

import com.barracuda.engine.WorkflowUnitTest;
import com.barracuda.engine.domain.WorkflowStatus;
import com.barracuda.engine.store.WorkflowStore;
import com.barracuda.engine.test.FailWork;
import com.barracuda.engine.test.TestCpuTask;
import com.barracuda.engine.work.SequentialWork;
import com.barracuda.engine.work.Work;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.*;

@WorkflowUnitTest
public class WorkflowTest extends AbstractWorkflowTest{

    @Mock
    private WorkflowStore storeMock;

    private final Duration cpuTimeSlot = Duration.ofMillis(100);
    private final ExecutorService cpuExecutorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private final ExecutorService virtualExecutorService = Executors.newVirtualThreadPerTaskExecutor();
    private final List<Work> works = List.of(new SequentialWork("TEST_SEQUENTIAL_WORK",1,List.of(new TestCpuTask("TEST_CPU_TASK",1,Duration.ofMillis(300)))));
    private final Workflow workflow = new RootWorkflowImpl("TEST_NAME",1, cpuTimeSlot, cpuExecutorService,storeMock,works);

    @Test
    void shouldBeAbleToStartTheWorkflow() {
        assertThat(workflow.status()).isEqualTo(WorkflowStatus.INITIALIZED);

        virtualExecutorService.submit(workflow::execute);

        Awaitility.await().untilAsserted(() -> assertThat(workflow.status()).isEqualTo(WorkflowStatus.RUNNING));

        Awaitility.await().atMost(Duration.ofSeconds(1)).until(() -> workflow.status().equals(WorkflowStatus.COMPLETED));
    }

    @Test
    void shouldBeAbleToPauseWorkflowWithInterruption(){
        var workflowTask = virtualExecutorService.submit(workflow::execute);
        Awaitility.await().pollInterval(Duration.ofMillis(10)).until(() -> workflow.status().equals(WorkflowStatus.RUNNING));

        workflowTask.cancel(true);

        Awaitility.await().untilAsserted(() -> assertThat(workflow.status()).isEqualTo(WorkflowStatus.PAUSED));
    }

    @Test
    void workflowShouldFailIfWorkFails() {
        var exception = new RuntimeException("FAILED_WORK");
        var workflow = new RootWorkflowImpl("FAIL_WORKFLOW", 1, cpuTimeSlot, cpuExecutorService, storeMock, List.of(new FailWork(exception)));

        assertThatCode(workflow::execute).hasCause(exception);
        assertThat(workflow.status()).isEqualTo(WorkflowStatus.FAILED);
    }

    @Test
    void shouldNotBeAbleToRunWorkflowIfAlreadyRunning() {
        virtualExecutorService.submit(workflow::execute);
        waitUntilStarted(workflow);

        assertThatThrownBy(workflow::execute).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldNotBeAbleToRunWorkflowIfCompleted() {
        virtualExecutorService.submit(workflow::execute);
        waitUntilFinished(workflow);

        assertThatThrownBy(workflow::execute).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldBeAbleToStartWorkflowIfPaused() {
        var workflowTask = virtualExecutorService.submit(workflow::execute);
        waitUntilStarted(workflow);
        workflowTask.cancel(true);
        waitUntilPaused(workflow);

        virtualExecutorService.submit(workflow::execute);

        waitUntilStarted(workflow);
        waitUntilFinished(workflow);
    }
}
