package com.barracuda.engine.test;

import com.barracuda.engine.event.ExecutionEvent;
import com.barracuda.engine.event.ExecutionEvent.ContinueEvent;
import com.barracuda.engine.utility.AwaitilityUtils;
import com.barracuda.engine.event.InMemoryEventCapturer;
import com.barracuda.engine.flow.Flow;
import com.barracuda.engine.flow.FlowState;

import java.time.Duration;
import java.util.List;
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
    private final Map<Class<?>, Map<String,TestTask<?>>> tasks;
    private final InMemoryEventCapturer eventCapturer;
    private Future<?> flowTask;
    private final Map<String,Long> subflows;

    public TestFlow(Flow flow, ExecutorService executorService, Map<Class<?>, Map<String,TestTask<?>>> tasks, InMemoryEventCapturer eventCapturer,Map<String,Long> subflows) {
        this.flow = flow;
        this.executorService = executorService;
        this.eventCapturer = eventCapturer;
        this.tasks = tasks;
        this.subflows = subflows;
    }

    public List<ExecutionEvent> events(){
        return eventCapturer.events();
    }

    public TestFlow startFlow() {
        flowTask = executorService.submit( () -> flow.event(new ContinueEvent(flow.id())));
        AwaitilityUtils.waitUntilFlowRunning(flow,Duration.ofSeconds(1));
        return this;
    }

    public TestFlow interruptFlowAndExpectFlowPaused() {
        flowTask.cancel(true);
        AwaitilityUtils.waitUntilFlowPaused(flow,Duration.ofSeconds(1));
        return this;
    }

    public TestFlow interruptFlowAndExpectFlowPaused(Duration duration) {
        flowTask.cancel(true);
        AwaitilityUtils.waitUntilFlowPaused(flow,duration);
        return this;
    }

    public TestFlow failTask(String taskName, RuntimeException exception) {
        getTestTaskByName(taskName).failNow(exception).waitUntilFailed(Duration.ofSeconds(1));
        return this;
    }

    public TestFlow finishTask(String taskName) {
        getTestTaskByName(taskName).finish().waitUntilCompleted(Duration.ofSeconds(1));
        return this;
    }

    public TestFlow expectIsRunning(){
        AwaitilityUtils.waitUntilFlowRunning(flow,Duration.ofSeconds(1));
        return this;
    }

    public TestFlow expectFlowInCreatedState(){
        assertThat(flow.state()).isEqualTo(FlowState.CREATED);
        return this;
    }

    public TestFlow expectFlowCompleted() {
        AwaitilityUtils.waitUntilFlowCompleted(flow,Duration.ofSeconds(1));
        return this;
    }

    public TestFlow expectFlowFailed() {
        AwaitilityUtils.waitUntilFlowFailed(flow,Duration.ofSeconds(1));
        return this;
    }

    public TestFlow expectFlowFailed(RuntimeException exception) {
        AwaitilityUtils.waitUntilFlowFailed(flow,Duration.ofSeconds(1));
        assertThatThrownBy(() -> flowTask.get()).hasCause(exception);
        return this;
    }

    private TestFlow assertThatTask(String taskName, Consumer<TestTaskVerifier> consumer) {
        consumer.accept(new TestTaskVerifier(getTestTaskByName(taskName)));

        return this;
    }

    public <T> TestFlow assertConsumerTaskInput(String taskName, Class<T> clazz, Consumer<TestTaskVerifier<T>> verifier) {
        verifier.accept(new TestTaskVerifier<>(getConsumerTaskByName(taskName, clazz)));
        return this;
    }

    public TestFlow assertTaskRunning(String taskName){
        return assertThatTask(taskName, TestTaskVerifier::isRunning);
    }

    public TestFlow assertTaskCancelled(String taskName){
        return assertThatTask(taskName, TestTaskVerifier::wasCancelled);
    }

    public TestFlow assertTaskNotStarted(String taskName){
        return assertThatTask(taskName, TestTaskVerifier::hasNotStarted);
    }

    public TestFlow assertTaskRanOnPlatformThread(String taskName){
        return assertThatTask(taskName, TestTaskVerifier::ranOnPlatformThread);
    }

    public TestFlow assertTaskRanOnVirtualThread(String taskName){
        return assertThatTask(taskName, TestTaskVerifier::ranOnVirtualThread);
    }

    public TestFlow assertFlowEventsInOrder(Consumer<FlowEventsInOrderVerifier> consumer) {
        consumer.accept(new FlowEventsInOrderVerifier(flow, eventCapturer.flowEvents(flow.id())));
        return this;
    }

    public TestFlow assertSubflowEventsInOrder(String subflow, Consumer<SubflowEventsInOrderVerifier> consumer) {
        long subflowID = Objects.requireNonNull(subflows.get(subflow), "Subflow with name " + subflow + " not found. Configured subflows: " + subflows.keySet());
        consumer.accept(new SubflowEventsInOrderVerifier(flow,eventCapturer.subflowEvents(flow.id(),subflowID),subflowID));
        return this;
    }

    public TestFlow assertTaskEventsInOrder(String taskName, Consumer<TaskEventsInOrderVerifier> consumer) {
        var task = getTestTaskByName(taskName);
        consumer.accept(new TaskEventsInOrderVerifier(task, eventCapturer.taskEvents(task.id())));
        return this;
    }

    private TestTask<Void> getTestTaskByName(String taskName) {
        return Objects.requireNonNull(getConsumerTaskByName(taskName, Void.class));
    }

    private <I> TestTask<I> getConsumerTaskByName(String taskName,Class<I> clazz) {
        Map<String,TestTask<?>> tasks = Objects.requireNonNull(this.tasks.get(clazz),"No task was found that accepts input of type " + clazz);

        var task = Objects.requireNonNull(tasks.get(taskName), "Task " + taskName + " not found that accept input of type " + clazz + ". Configured tasks that accept input of type " + clazz + ": " + tasks.keySet());

        return (TestTask<I>) task;
    }

}
