package com.barracuda.engine.workflow;

import com.barracuda.engine.domain.WorkflowStatus;
import com.barracuda.engine.event.WorkflowEventPublisher;
import com.barracuda.engine.event.WorkflowEventPublisherImpl;
import com.barracuda.engine.store.WorkflowStore;
import com.barracuda.engine.test.TestCpuTask;
import com.barracuda.engine.work.SequentialWork;
import com.barracuda.engine.work.Work;
import org.mockito.Mock;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AbstractWorkflowTest {

    @Mock
    WorkflowStore storeMock;

    final Duration cpuTimeSlot = Duration.ofMillis(100);
    final ExecutorService cpuExecutorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    final ExecutorService virtualExecutorService = Executors.newVirtualThreadPerTaskExecutor();
    final WorkflowEventPublisher eventPublisher = new WorkflowEventPublisherImpl();
    final List<Work> works = List.of(new SequentialWork("TEST_SEQUENTIAL_WORK",1,List.of(new TestCpuTask("TEST_CPU_TASK",1,Duration.ofMillis(300)))));
    final Workflow workflow = new RootWorkflowImpl("TEST_NAME",1, cpuTimeSlot, cpuExecutorService,storeMock,works,eventPublisher);

    Workflow withWork(Work... works) {
        return new RootWorkflowImpl("TEST_NAME",1, cpuTimeSlot, cpuExecutorService,storeMock, List.of(works),eventPublisher);
    }

    void waitUntilStarted(Workflow workflow) {
        Awaitility.await().pollInterval(Duration.ofMillis(10)).until(() -> workflow.status().equals(WorkflowStatus.RUNNING));
    }

    void waitUntilFinished(Workflow workflow) {
        Awaitility.await().pollInterval(Duration.ofMillis(10)).until(() -> workflow.status().equals(WorkflowStatus.COMPLETED));
    }

    void waitUntilPaused(Workflow workflow) {
        Awaitility.await().pollInterval(Duration.ofMillis(10)).until(() -> workflow.status().equals(WorkflowStatus.PAUSED));
    }


}
