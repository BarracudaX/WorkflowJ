package com.barracuda.engine.test.flow;

import com.barracuda.engine.event.ExecutionEvent;
import com.barracuda.engine.event.ExecutionEvent.CommandEvent.Continue;
import com.barracuda.engine.event.ExecutionEvent.CommandEvent.EnterReplayMode;
import com.barracuda.engine.event.ExecutionEvent.CommandEvent.Reset;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent;
import com.barracuda.engine.event.InMemoryEventCapturer;
import com.barracuda.engine.flow.Flow;
import com.barracuda.engine.flow.FlowState;
import com.barracuda.engine.test.task.TaskEventsInOrderVerifier;
import com.barracuda.engine.test.task.TestTask;
import com.barracuda.engine.test.task.TestTaskVerifier;
import com.barracuda.engine.utility.AwaitilityUtils;
import org.assertj.core.api.AbstractThrowableAssert;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestFlow {

    private final InMemoryEventCapturer eventCapturer;
    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
    private final Flow flow;
    private final Map<String,TestFlow> subflows;
    private final Map<Class<?>, Map<String, TestTask<?>>> tasks;
    private Future<?> flowTask;

    public TestFlow(InMemoryEventCapturer eventCapturer, Flow flow, Map<String, TestFlow> subflows, Map<Class<?>, Map<String, TestTask<?>>> tasks) {
        this.eventCapturer = eventCapturer;
        this.flow = flow;
        this.subflows = subflows;
        this.tasks = tasks;
    }

    public List<SubflowEvent> subflowEvents(String subflowName) {
        return eventCapturer.subflowEvents(flow.id(), subflowID(subflowName));
    }

    public long subflowID(String subflowName){
        return getSubflowByName(subflowName).flowID();
    }

    public long flowID(){
        return flow.id();
    }

    public TestFlow replayMode(){
        flow.event(new EnterReplayMode());
        return this;
    }

    public TestFlow reset(){
        flow.event(new Reset());
        return this;
    }

    public TestFlow sendStartEvent(){
        flow.event(new FlowEvent.FlowStartedEvent(flow.id()));
        return this;
    }

    public TestFlow startFlow() {
        flowTask = executorService.submit( () -> flow.event(new Continue()));
        AwaitilityUtils.waitUntilFlowRunning(flow, Duration.ofSeconds(1));
        return this;
    }

    public void startSync(){
        try {
            executorService.submit( () -> flow.event(new Continue())).get();
        } catch (InterruptedException | ExecutionException e) {
            if(e.getCause() instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new RuntimeException(e);
        }
    }

    public TestFlow assertThrows(Consumer<TestFlow> consumer, Consumer<AbstractThrowableAssert<? extends AbstractThrowableAssert<?,?>,?>> assertionConsumer){
        assertionConsumer.accept(assertThatThrownBy(() -> consumer.accept(this)));
        return this;
    }

    /**
     * Starts the flow. Ignores if the flow transitions to COMPLETED state because it's empty or the configured task completes faster than the
     * code gets to see the RUNNING state of the flow.
     */
    public TestFlow startFlowIgnoreIfCompleted(){
        try{
            return startFlow();
        }catch (AssertionError ex){
            return expectFlowCompleted();
        }
    }

    public TestFlow interruptFlowAndExpectFlowPaused() {
        interruptFlowAndExpectFlowPaused(Duration.ofSeconds(1));
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

    public TestFlow expectFlowReady(){
        assertThat(flow.state()).isEqualTo(FlowState.READY);
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

    public TestFlow expectFlowInReplayMode(){
        AwaitilityUtils.waitUntilFlowInReplayMode(flow,Duration.ofSeconds(1));
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

    public TestFlow expectTaskIsRunning(String taskName){
        return assertTaskRunning(taskName);
    }

    public TestFlow expectTaskCancelled(String taskName){
        return assertThatTask(taskName, TestTaskVerifier::wasCancelled);
    }

    public TestFlow expectTaskNotStarted(String taskName){
        return assertThatTask(taskName, TestTaskVerifier::hasNotStarted);
    }

    public TestFlow expectTaskRanOnPlatformThread(String taskName){
        return assertThatTask(taskName, TestTaskVerifier::ranOnPlatformThread);
    }

    public TestFlow expectTaskRanOnVirtualThread(String taskName){
        return assertThatTask(taskName, TestTaskVerifier::ranOnVirtualThread);
    }

    public TestFlow assertFlowEventsInOrder(Consumer<FlowEventsInOrderVerifier> consumer) {
        consumer.accept(new FlowEventsInOrderVerifier(flow, eventCapturer.flowEvents(flow.id())));
        return this;
    }

    public TestFlow assertTaskEventsInOrder(String taskName, Consumer<TaskEventsInOrderVerifier> consumer) {
        var task = getTestTaskByName(taskName);
        consumer.accept(new TaskEventsInOrderVerifier(task, eventCapturer.taskEvents(task.id())));
        return this;
    }

    public TestFlow assertSubflowEventsInOrder(String subflow, Consumer<SubflowEventsInOrderVerifier> consumer) {
        long subflowID = getSubflowByName(subflow).flow.id();
        consumer.accept(new SubflowEventsInOrderVerifier(flow,eventCapturer.subflowEvents(flow.id(),subflowID),subflowID));
        return this;
    }

    public SubflowEventsInOrderVerifier getSubflowEventsVerifier(String subflow) {
        long subflowID = getSubflowByName(subflow).flow.id();
        return new SubflowEventsInOrderVerifier(flow, eventCapturer.subflowEvents(flow.id(), subflowID), subflowID);
    }

    private TestTask<Void> getTestTaskByName(String taskName) {
        try {
            return Objects.requireNonNull(getConsumerTaskByName(taskName, Void.class));
        }catch (NullPointerException e) {
            for(var subflow : subflows.values()) {
                try{
                    return subflow.getTestTaskByName(taskName);
                }catch (NullPointerException _){}
            }
            throw e;
        }
    }

    private <I> TestTask<I> getConsumerTaskByName(String taskName,Class<I> clazz) {
        Map<String,TestTask<?>> tasks = Objects.requireNonNull(this.tasks.get(clazz),"No task was found that accepts input of type " + clazz);

        var task = Objects.requireNonNull(tasks.get(taskName), "Task " + taskName + " not found that accept input of type " + clazz + ". Configured tasks that accept input of type " + clazz + ": " + tasks.keySet());

        return (TestTask<I>) task;
    }

    private TestFlow getSubflowByName(String subflowName) {
        try {
            return Objects.requireNonNull(subflows.get(subflowName), "Subflow with name " + subflowName + " not found. Configured subflows: " + subflows.keySet());
        } catch (NullPointerException ex) {
            for(var subflow : subflows.values()) {
                try{
                    return subflow.getSubflowByName(subflowName);
                }catch (NullPointerException _){}
            }
            throw ex;
        }
    }
}
