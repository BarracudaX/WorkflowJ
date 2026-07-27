package com.barracuda.engine.test.flow;

import com.barracuda.engine.event.Command.Continue;
import com.barracuda.engine.event.Command.Prepare;
import com.barracuda.engine.event.Command.Reset;
import com.barracuda.engine.event.ExecutionEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowStartedEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent;
import com.barracuda.engine.event.ExecutionEvent.TaskEvent;
import com.barracuda.engine.event.InMemoryEventCapturer;
import com.barracuda.engine.flow.Flow;
import com.barracuda.engine.flow.FlowPrettyOutput;
import com.barracuda.engine.test.task.TestTask;
import com.barracuda.engine.utility.AwaitilityUtils;
import lombok.Getter;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public class TestFlow {

    private final InMemoryEventCapturer eventCapturer;
    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
    @Getter
    private final Flow flow;
    private final Map<String,TestFlow> subflows;
    private final Map<String, TestTask> tasks;
    @Getter
    private Future<?> flowTask;

    public TestFlow(InMemoryEventCapturer eventCapturer, Flow flow, Map<String, TestFlow> subflows, Map<String, TestTask> tasks) {
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
        return eventCapturer.taskEvents(getTestTaskByName(taskName).id());
    }

    public long subflowID(String subflowName){
        return getSubflowByName(subflowName).flowID();
    }

    public long flowID(){
        return flow.id();
    }


    public TestFlow prepare(){
        flow.command(new Prepare());
        return this;
    }

    public TestFlow sendEvent(ExecutionEvent event) {
        flow.event(event);
        return this;
    }

    public TestFlow sendFlowStartedEvent(){
        flow.event(new FlowStartedEvent(flow.id()));
        return this;
    }

    public TestFlow reset(){
        flow.command(new Reset());
        return this;
    }

    public TestFlow startFlow() {
        flowTask = executorService.submit( () -> flow.command(new Continue()));
        runCatching(() -> AwaitilityUtils.waitUntilFlowRunning(flow,Duration.ofSeconds(1)));
        return this;
    }

    public void startSync(){
        try {
            executorService.submit( () -> flow.command(new Continue())).get();
        } catch (InterruptedException | ExecutionException e) {
            if(e.getCause() instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new RuntimeException(e);
        }
    }

    public TestFlow interruptFlow() {
        runCatching(() -> flowTask.cancel(true));
        runCatching(() -> AwaitilityUtils.waitUntilFlowPaused(flow,Duration.ofSeconds(1)));
        return this;
    }

    public TestFlow failTask(String taskName, RuntimeException exception) {
        TestTask task = getTestTaskByName(taskName);
        task.failNow(exception);
        runCatching(() -> AwaitilityUtils.waitUntilTestTaskFailed(task,Duration.ofSeconds(1)));
        runCatching(() -> AwaitilityUtils.waitUntilFlowFailed(flow,Duration.ofSeconds(1)));
        return this;
    }

    public TestFlow finishTask(String taskName) {
        TestTask task = getTestTaskByName(taskName);

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

        output
                .getStringBuilder()
                .append("\n\n")
                .append("Recorded Events: [")
                .append(eventCapturer.events())
                .append("]");

        try {
            flowTask.get(1, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException e) {
            output.getStringBuilder().append("\nFlow Exception: \n").append(org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace(e)).append("\n");
        }catch (CancellationException _){
            // ignore cancellation
        } catch (TimeoutException e) {
            output.getStringBuilder().append("\nUnable to get flow result's in 1 second. The flow is still running.");
        }

        return output.toString();
    }

    public TestFlow waitUntilTaskRunning(String taskName) {
        runCatching(() -> AwaitilityUtils.waitUntilTestTaskIsRunning(getTestTaskByName(taskName), Duration.ofSeconds(1)));
        return this;
    }

    public SequencedCollection<FlowEvent> flowEvents(){
        return eventCapturer.flowEvents(flow.id());
    }

    public TestTask getTestTaskByName(String taskName) {
        try {
            return Objects.requireNonNull(tasks.get(taskName), "Task " + taskName +". Configured tasks: " + tasks.keySet());
        }catch (NullPointerException e) {
            for(var subflow : subflows.values()) {
                try{
                    return subflow.getTestTaskByName(taskName);
                }catch (NullPointerException _){}
            }
            throw e;
        }
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
