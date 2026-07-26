package com.barracuda.engine.test.flow;

import com.barracuda.engine.event.ExecutionEvent;
import com.barracuda.engine.event.ExecutionEvent.CommandEvent.Continue;
import com.barracuda.engine.event.ExecutionEvent.CommandEvent.EnterReplayMode;
import com.barracuda.engine.event.ExecutionEvent.CommandEvent.Prepare;
import com.barracuda.engine.event.ExecutionEvent.CommandEvent.Reset;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent;
import com.barracuda.engine.event.ExecutionEvent.TaskEvent;
import com.barracuda.engine.event.InMemoryEventCapturer;
import com.barracuda.engine.flow.Flow;
import com.barracuda.engine.flow.FlowPrettyOutput;
import com.barracuda.engine.test.task.TestTask;
import com.barracuda.engine.utility.AwaitilityUtils;
import lombok.Getter;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SequencedCollection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestFlow {

    private final InMemoryEventCapturer eventCapturer;
    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
    @Getter
    private final Flow flow;
    private final Map<String,TestFlow> subflows;
    private final Map<Class<?>, Map<String, TestTask<?>>> tasks;
    @Getter
    private Future<?> flowTask;

    public TestFlow(InMemoryEventCapturer eventCapturer, Flow flow, Map<String, TestFlow> subflows, Map<Class<?>, Map<String, TestTask<?>>> tasks) {
        this.eventCapturer = eventCapturer;
        this.flow = flow;
        this.subflows = subflows;
        this.tasks = tasks;
    }

    public TestFlow subflow(String subflowName) {
        try {
            return Objects.requireNonNull(subflows.get(subflowName), "No subflow found with name " + subflowName);
        } catch (NullPointerException e) {
            for (var subflow : subflows.values()) {
                try {
                    return subflow.subflow(subflowName);
                } catch (NullPointerException _) {
                }
            }
            throw e;
        }
    }

    public List<SubflowEvent> subflowEvents(String subflowName) {
        return eventCapturer.subflowEvents(flow.id(), subflowID(subflowName));
    }

    public List<TaskEvent> taskEvents(String taskName) {
        return eventCapturer.taskEvents(getConsumerTaskByName(taskName, Void.class).id());
    }

    public long subflowID(String subflowName){
        return getSubflowByName(subflowName).flowID();
    }

    public long flowID(){
        return flow.id();
    }

    public TestFlow enterReplayMode(){
        flow.event(new EnterReplayMode());
        return this;
    }

    public TestFlow prepare(){
        flow.event(new Prepare());
        return this;
    }

    public TestFlow sendEvent(ExecutionEvent event) {
        flow.event(event);
        return this;
    }

    public TestFlow sendFlowStartedEvent(){
        flow.event(new FlowEvent.FlowStartedEvent(flow.id()));
        return this;
    }

    public TestFlow reset(){
        flow.event(new Reset());
        return this;
    }

    public TestFlow startFlow() {
        flowTask = executorService.submit( () -> flow.event(new Continue()));
        runCatching(() -> AwaitilityUtils.waitUntilFlowRunning(flow,Duration.ofSeconds(1)));
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

    public TestFlow interruptFlow() {
        flowTask.cancel(true);
        runCatching(() -> AwaitilityUtils.waitUntilFlowPaused(flow,Duration.ofSeconds(1)));
        return this;
    }

    public TestFlow waitUntilTaskRunningAndFailItAndWaitUntilFailed(String taskName, RuntimeException exception) {
        waitUntilTaskRunning(taskName);
        failTask(taskName, exception);
        waitUntilTaskFailed(taskName);

        return runCatching(() -> AwaitilityUtils.waitUntilFlowFailed(flow,Duration.ofSeconds(1)));
    }

    public TestFlow failTask(String taskName, RuntimeException exception) {
        TestTask<Void> task = getTestTaskByName(taskName);
        task.failNow(exception);
        runCatching(() -> AwaitilityUtils.waitUntilTestTaskFailed(task,Duration.ofSeconds(1)));
        runCatching(() -> AwaitilityUtils.waitUntilFlowFailed(flow,Duration.ofSeconds(1)));
        return this;
    }

    public TestFlow finishTask(String taskName) {
        TestTask<Void> task = getTestTaskByName(taskName);

        task.finish();

        runCatching(() -> AwaitilityUtils.waitUntilTestTaskCompleted(task,Duration.ofSeconds(1)));

        return this;
    }

    public TestFlow waitUntilPaused(){
        return runCatching(() -> AwaitilityUtils.waitUntilFlowPaused(flow, Duration.ofSeconds(1)));
    }

    public TestFlow waitUntilFlowCompleted() {
        return runCatching(() -> AwaitilityUtils.waitUntilFlowCompleted(flow, Duration.ofSeconds(1)));
    }

    private TestFlow runCatching(Runnable runnable){
        try {
            runnable.run();
        } catch (Throwable ex) {
            System.err.println(context());
            throw ex;
        }

        return this;
    }

    public String context(){
        FlowPrettyOutput output =  new FlowPrettyOutput();

        flow.prettyPrint(output);

        return output
                .getStringBuilder()
                .append("\n\n")
                .append("Recorded Events: [")
                .append(eventCapturer.events())
                .append("]")
                .toString();
    }

    public TestFlow waitUntilTaskFailed(String taskName) {
        runCatching(() -> AwaitilityUtils.waitUntilTestTaskFailed(getTestTaskByName(taskName),Duration.ofSeconds(1)));
        return this;
    }

    public TestFlow waitUntilTaskRunning(String taskName) {
        runCatching(() -> AwaitilityUtils.waitUntilTestTaskIsRunning(getTestTaskByName(taskName), Duration.ofSeconds(1)));
        return this;
    }

    public SequencedCollection<FlowEvent> flowEvents(){
        return eventCapturer.flowEvents(flow.id());
    }

    public TestTask<Void> getTestTaskByName(String taskName) {
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

    public <I> TestTask<I> getConsumerTaskByName(String taskName,Class<I> clazz) {
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
