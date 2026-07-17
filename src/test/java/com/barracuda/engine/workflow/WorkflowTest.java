package com.barracuda.engine.workflow;

import com.barracuda.engine.WorkflowUnitTest;
import com.barracuda.engine.domain.WorkflowStatus;
import com.barracuda.engine.test.FailWork;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.*;

@WorkflowUnitTest
public class WorkflowTest extends AbstractWorkflowTest{

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
        var workflow = withWork(new FailWork(exception));

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
