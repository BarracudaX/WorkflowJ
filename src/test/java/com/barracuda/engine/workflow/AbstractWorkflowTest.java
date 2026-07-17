package com.barracuda.engine.workflow;

import com.barracuda.engine.domain.WorkflowStatus;
import com.barracuda.engine.event.WorkflowEventPublisher;
import com.barracuda.engine.store.WorkflowStore;
import com.barracuda.engine.test.TestCpuTask;
import com.barracuda.engine.work.SequentialWork;
import com.barracuda.engine.work.Work;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AbstractWorkflowTest {

    @Mock
    WorkflowStore storeMock;
    @Mock
    WorkflowEventPublisher eventPublisherMock;
    Workflow workflow;

    final Duration cpuTimeSlot = Duration.ofMillis(100);
    final ExecutorService cpuExecutorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    final ExecutorService virtualExecutorService = Executors.newVirtualThreadPerTaskExecutor();
    final List<Work> works = List.of(new SequentialWork("TEST_SEQUENTIAL_WORK",1,List.of(new TestCpuTask("TEST_CPU_TASK",1,Duration.ofMillis(300)))));

    @BeforeEach
    void setUp() {
        workflow = new RootWorkflowImpl("TEST_NAME",1, cpuTimeSlot, cpuExecutorService,storeMock,works, eventPublisherMock);
    }

    Workflow withWork(Work... works) {
        return new RootWorkflowImpl("TEST_NAME",1, cpuTimeSlot, cpuExecutorService,storeMock, List.of(works), eventPublisherMock);
    }

    void waitUntilStarted(Workflow workflow) {
        Awaitility.await().pollInterval(Duration.ofMillis(10)).atMost(Duration.ofSeconds(1)).until(() -> workflow.status().equals(WorkflowStatus.RUNNING));
    }

    void waitUntilFinished(Workflow workflow) {
        Awaitility.await().pollInterval(Duration.ofMillis(10)).atMost(Duration.ofSeconds(1)).until(() -> workflow.status().equals(WorkflowStatus.COMPLETED));
    }

    void waitUntilPaused(Workflow workflow) {
        Awaitility.await().pollInterval(Duration.ofMillis(10)).atMost(Duration.ofSeconds(1)).until(() -> workflow.status().equals(WorkflowStatus.PAUSED));
    }

    void waitUntilFailed(Workflow workflow) {
        Awaitility.await().pollInterval(Duration.ofMillis(10)).atMost(Duration.ofSeconds(1)).until(() -> workflow.status().equals(WorkflowStatus.FAILED));
    }

    Object block(InvocationOnMock invocation){

        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        return null;
    }

}
