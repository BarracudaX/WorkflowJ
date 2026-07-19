package com.barracuda.engine.test;

import com.barracuda.engine.utility.AwaitilityUtils;
import com.barracuda.engine.event.InMemoryEventCapturer;
import com.barracuda.engine.flow.Flow;
import com.barracuda.engine.flow.FlowState;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public final class TestFlow {

    private final Flow flow;
    private final ExecutorService executorService;
    private final Map<String, TestTask> tasks;
    private final InMemoryEventCapturer eventCapturer;
    private Future<?> flowTask;

    public TestFlow(Flow flow, ExecutorService executorService, Map<String, TestTask> tasks, InMemoryEventCapturer eventCapturer) {
        this.flow = flow;
        this.executorService = executorService;
        this.tasks = tasks;
        this.eventCapturer = eventCapturer;
    }

    public TestFlow startFlow() {
        flowTask = executorService.submit(flow::execute);
        AwaitilityUtils.waitUntilFlowRunning(flow);
        return this;
    }

    public TestFlow interruptFlow() {
        flowTask.cancel(true);
        AwaitilityUtils.waitUntilFlowPaused(flow);
        return this;
    }

    public TestFlow failTask(String taskName, RuntimeException exception) {
        getTaskByName(taskName).failNow(exception).waitUntilFailed();
        return this;
    }

    public TestFlow finishTask(String taskName) {
        getTaskByName(taskName).finish().waitUntilCompleted();
        return this;
    }

    public TestFlow expectIsRunning(){
        AwaitilityUtils.waitUntilFlowRunning(flow);
        return this;
    }

    public TestFlow expectFlowInCreatedState(){
        assertThat(flow.state()).isEqualTo(FlowState.CREATED);
        return this;
    }

    public TestFlow expectFlowPaused() {
        AwaitilityUtils.waitUntilFlowPaused(flow);
        return this;
    }

    public TestFlow expectFlowCompleted() {
        AwaitilityUtils.waitUntilFlowCompleted(flow);
        return this;
    }

    public TestFlow expectFlowFailed() {
        AwaitilityUtils.waitUntilFlowFailed(flow);
        return this;
    }

    public TestFlow expectFlowFailed(RuntimeException exception) {
        AwaitilityUtils.waitUntilFlowFailed(flow);
        assertThatThrownBy(() -> flowTask.get()).hasCause(exception);
        return this;
    }

    public TestFlow assertThatTask(String taskName, Consumer<TestTaskVerifier> consumer) {
        consumer.accept(new TestTaskVerifier(getTaskByName(taskName)));

        return this;
    }

    public TestFlow assertFlowEventsInOrder(Consumer<FlowEventsInOrderVerifier> consumer) {
        consumer.accept(new FlowEventsInOrderVerifier(flow, eventCapturer.flowEvents(flow.id())));
        return this;
    }

    public TestFlow assertTaskEventsInOrder(String taskName, Consumer<TaskEventsInOrderVerifier> consumer) {
        var task = getTaskByName(taskName);
        consumer.accept(new TaskEventsInOrderVerifier(task, eventCapturer.taskEvents(task.id())));
        return this;
    }

    private TestTask getTaskByName(String taskName) {
        return Objects.requireNonNull(tasks.get(taskName), "Task " + taskName + " not found. Configured tasks:" + tasks.keySet());
    }

}
