package com.barracuda.engine.flow;

import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowCompletedEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowStartedEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.SubflowCompletedEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.SubflowStartedEvent;
import com.barracuda.engine.event.ExecutionEvent.TaskEvent.TaskCompletedEvent;
import com.barracuda.engine.event.ExecutionEvent.TaskEvent.TaskStartEvent;
import com.barracuda.engine.test.assertJ.TestFlowAssert.TestTaskAssert;
import com.barracuda.engine.test.builder.TestFlowBuilder;
import com.barracuda.engine.test.flow.TestFlow;
import org.assertj.core.api.AbstractIterableAssert;
import org.junit.jupiter.api.Test;

import static com.barracuda.engine.test.assertJ.CustomAssertions.assertThat;

public class FlowReplayTest {

    private final TestFlow testFlow = TestFlowBuilder
            .testFlow()
            .actionTask("task1")
            .parallel(parallel ->
                parallel
                        .subflow("Subflow1",subflow -> subflow.actionTask("subflowTask1"))
                        .subflow("Subflow2",subflow -> subflow.actionTask("subflowTask2"))
                        .subflow("Subflow3",subflow -> subflow.actionTask("subflowTask3"))
            )
            .actionTask("task2")
            .build();

    private final long rootID = testFlow.flowID();
    private final long subflow_1_ID = testFlow.subflowID("Subflow1");
    private final long subflow_2_ID = testFlow.subflowID("Subflow2");
    private final long subflow_3_ID = testFlow.subflowID("Subflow3");
    private final long task_1_ID = testFlow.taskID("task1");
    private final long subflow_task_1_ID = testFlow.taskID("subflowTask1");
    private final long subflow_task_2_ID = testFlow.taskID("subflowTask2");
    private final long subflow_task_3_ID = testFlow.taskID("subflowTask3");
    private final long task_2_ID = testFlow.taskID("task2");

    @Test
    void shouldRunTestFlowNormally() {
        testFlow.startFlow()
                .finishTask("task1")
                .finishTask("subflowTask1")
                .finishTask("subflowTask2")
                .finishTask("subflowTask3")
                .finishTask("task2");

        assertThat(testFlow)
                .hasTaskSatisfying("task1", TestTaskAssert::hasEventuallyCompleted)
                .hasTaskSatisfying("subflowTask1", TestTaskAssert::hasEventuallyCompleted)
                .hasTaskSatisfying("subflowTask2", TestTaskAssert::hasEventuallyCompleted)
                .hasTaskSatisfying("subflowTask3", TestTaskAssert::hasEventuallyCompleted);

        assertThat(testFlow)
                .executionHistory(history -> history
                                .containsSequence(new TaskStartEvent(rootID, task_1_ID), new TaskCompletedEvent(rootID, task_1_ID))
                                .containsSequence(new TaskStartEvent(rootID, task_2_ID), new TaskCompletedEvent(rootID, task_2_ID))
                                .containsSubsequence(new TaskStartEvent(subflow_1_ID, subflow_task_1_ID), new TaskCompletedEvent(subflow_1_ID, subflow_task_1_ID))
                                .containsSubsequence(new TaskStartEvent(subflow_2_ID, subflow_task_2_ID), new TaskCompletedEvent(subflow_2_ID, subflow_task_2_ID))
                                .containsSubsequence(new TaskStartEvent(subflow_3_ID, subflow_task_3_ID), new TaskCompletedEvent(subflow_3_ID, subflow_task_3_ID))
                                .containsSubsequence(new FlowStartedEvent(rootID),new FlowCompletedEvent(rootID))
                                .containsSubsequence(new SubflowStartedEvent(rootID,subflow_1_ID),new SubflowCompletedEvent(rootID,subflow_1_ID))
                                .containsSubsequence(new SubflowStartedEvent(rootID,subflow_2_ID),new SubflowCompletedEvent(rootID,subflow_2_ID))
                                .containsSubsequence(new SubflowStartedEvent(rootID,subflow_3_ID),new SubflowCompletedEvent(rootID,subflow_3_ID))
                                .satisfies(actual -> assertThat(actual).first().isEqualTo(new FlowStartedEvent(rootID)))
                                .satisfies(actual -> assertThat(actual).last().isEqualTo(new FlowCompletedEvent(rootID)))
                );
    }

    @Test
    void shouldNotPublishFlowStartedEventIfItWasReplayed() {
        testFlow.sendEvent(new FlowStartedEvent(rootID)); // replay event as if happened

        testFlow.startFlow();

        assertThat(testFlow).executionHistory(history -> history.doesNotContain(new FlowStartedEvent(rootID)));
    }

    @Test
    void shouldNotExecuteTaskIfItWasCompletedWithReplay() {
        testFlow.sendEvent(new TaskCompletedEvent(rootID, task_1_ID));

        testFlow.startFlow();

        assertThat(testFlow).hasTaskSatisfying("task1", TestTaskAssert::hasNotStarted);

        assertThat(testFlow).taskEventsSatisfying("task1", AbstractIterableAssert::isEmpty);
    }

    @Test
    void shouldExecuteTheNextTaskWhenSkippedOneWithReplay() {
        TestFlow testFlow = TestFlowBuilder
                .testFlow()
                .actionTask("task1")
                .actionTask("task2")
                .build();

        var skippedTaskID = testFlow.taskID("task1");

        testFlow.sendEvent(new TaskCompletedEvent(rootID, skippedTaskID));

        testFlow.startFlow();

        assertThat(testFlow).hasTaskSatisfying("task1", TestTaskAssert::hasNotStarted);
        assertThat(testFlow).hasTaskSatisfying("task2", TestTaskAssert::isRunning);
    }

    @Test
    void shouldSkipSubflowTaskIfItWasCompletedWithReplay() {
        TestFlow testFlow = TestFlowBuilder
                .testFlow()
                .parallel( parallel -> parallel.subflow("subflow", subflow -> subflow.actionTask("task1").actionTask("task2")))
                .build();

        var skippedTaskID = testFlow.taskID("task1");
        var subflowID =  testFlow.subflowID("subflow");
        testFlow.sendEvent(new TaskCompletedEvent(subflowID, skippedTaskID));

        testFlow.startFlow();

        assertThat(testFlow).hasTaskSatisfying("task1", TestTaskAssert::hasNotStarted);
        assertThat(testFlow).hasTaskSatisfying("task2", TestTaskAssert::isRunning);
    }

    @Test
    void shouldSkipSubflowIfItWasCompletedWithReplay() {
        TestFlow testFlow = TestFlowBuilder
                .testFlow()
                .parallel( parallel -> parallel.subflow("subflow", subflow -> subflow.actionTask("subflowTask")))
                .actionTask("nextTask")
                .build();

        var rootID = testFlow.flowID();
        var subflowID =  testFlow.subflowID("subflow");
        var subflowTaskID = testFlow.taskID("subflowTask");
        testFlow.sendEvent(new TaskCompletedEvent(subflowID, subflowTaskID));
        testFlow.sendEvent(new SubflowCompletedEvent(rootID, subflowID));

        testFlow.startFlow();

        assertThat(testFlow).hasTaskSatisfying("subflowTask", TestTaskAssert::hasNotStarted);
        assertThat(testFlow).hasTaskSatisfying("nextTask", TestTaskAssert::isRunning);
    }
}
