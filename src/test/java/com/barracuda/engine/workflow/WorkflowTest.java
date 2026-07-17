package com.barracuda.engine.workflow;

import com.barracuda.engine.WorkflowUnitTest;
import com.barracuda.engine.domain.WorkflowStatus;
import com.barracuda.engine.event.WorkflowEvent.*;
import com.barracuda.engine.test.FailWork;
import com.barracuda.engine.work.Work;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

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
        var workflow = withWork(new FailWork(exception,1L));

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

    @Test
    void shouldPublishStartEventWhenStartingWorkflow() {
        verify(eventPublisherMock,never()).fire(any());
        virtualExecutorService.submit(workflow::execute);
        waitUntilStarted(workflow);

        verify(eventPublisherMock).fire(new WorkflowStartedEvent(workflow.id()));
    }

    @Test
    void shouldPublishCompletedEventWhenWorkflowFinishesExecuting() {
        verify(eventPublisherMock,never()).fire(any());
        virtualExecutorService.submit(workflow::execute);

        waitUntilFinished(workflow);

        verify(eventPublisherMock).fire(new WorkflowCompletedEvent(workflow.id()));
    }

    @Test
    void shouldPublishFailureEventWhenWorkflowFails() {
        verify(eventPublisherMock, never()).fire(any());
        var failedWork = new FailWork(new RuntimeException("FAILED"), 1L);
        var failedWorkflow = withWork(failedWork);
        virtualExecutorService.submit(failedWorkflow::execute);

        waitUntilFailed(failedWorkflow);

        verify(eventPublisherMock).fire(new WorkflowFailedEvent(failedWork.getException(),failedWork.id()));
    }

    @Test
    void shouldPublishResumedEventWhenWorkflowIsResumed() {
        var workflowTask = virtualExecutorService.submit(workflow::execute);
        waitUntilStarted(workflow);
        workflowTask.cancel(true);
        waitUntilPaused(workflow);

        virtualExecutorService.submit(workflow::execute);
        waitUntilStarted(workflow);

        verify(eventPublisherMock).fire(new WorkflowResumedEvent(workflow.id()));
    }

    @Test
    void shouldPublishWorkflowPausedEventWhenWorkflowIsPaused() {
        var workflowTask = virtualExecutorService.submit(workflow::execute);
        waitUntilStarted(workflow);
        workflowTask.cancel(true);
        waitUntilPaused(workflow);

        verify(eventPublisherMock).fire(new WorkflowPausedEvent(workflow.id()));
    }

    @Test
    void shouldRunWorksInOrder() {
        Work firstWork = mock(Work.class,"FirstWork"), secondWork = mock(Work.class,"SecondWork"), thirdWork = mock(Work.class,"ThirdWork");
        var inOrder = inOrder(firstWork, secondWork, thirdWork);
        var workflow = withWork(firstWork,secondWork,thirdWork);

        workflow.execute();

        inOrder.verify(firstWork).execute();
        inOrder.verify(secondWork).execute();
        inOrder.verify(thirdWork).execute();
    }

    @Test
    void shouldResumeExecutionWhereStopped() {
        Work firstWork = mock(Work.class,"FirstWork"), secondWork = mock(Work.class,"SecondWork"), thirdWork = mock(Work.class,"ThirdWork");
        var inOrder = inOrder(firstWork, secondWork, thirdWork);
        var workflow = withWork(firstWork,secondWork,thirdWork);
        doAnswer(this::block).doNothing().when(secondWork).execute(); // makes the second work block on first run and return immediately on the second run.

        var workflowTask = virtualExecutorService.submit(workflow::execute);
        waitUntilStarted(workflow);
        workflowTask.cancel(true);
        waitUntilPaused(workflow);

        workflow.execute();

        inOrder.verify(firstWork).execute();
        inOrder.verify(secondWork,times(2)).execute();
        inOrder.verify(thirdWork).execute();
    }

}
