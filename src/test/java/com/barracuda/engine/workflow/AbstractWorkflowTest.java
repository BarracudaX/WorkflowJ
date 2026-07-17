package com.barracuda.engine.workflow;

import com.barracuda.engine.domain.WorkflowStatus;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.time.Duration;

public class AbstractWorkflowTest {

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
