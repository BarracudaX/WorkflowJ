package com.barracuda.engine.test;

import com.barracuda.engine.builder.RootFlowBuilder;
import com.barracuda.engine.event.EvenPublisherImpl;
import com.barracuda.engine.event.FlowEventPublisher;
import com.barracuda.engine.event.InMemoryEventCapturer;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.barracuda.engine.utility.TestUtils.randomID;

public class TestFlowBuilder {

    private final ExecutorService cpuExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private final ExecutorService ioExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final InMemoryEventCapturer eventCapturer = new InMemoryEventCapturer();
    private final FlowEventPublisher evenPublisher = new EvenPublisherImpl();
    private final RootFlowBuilder rootFlowBuilder = new RootFlowBuilder(cpuExecutor, ioExecutor,evenPublisher).withID(randomID());
    private final Map<String, TestTask> tasks = new LinkedHashMap<>();

    private TestFlowBuilder() {
        evenPublisher.subscribe(eventCapturer);
    }
    /**
     * Creates parallel flows each with a single test task.
     */
    public TestFlowBuilder parallelFlows(String... parallelTaskNames) {

        rootFlowBuilder.parallel(parallel -> {
            for (String name : parallelTaskNames) {
                parallel.subflow(flow -> {
                    var task = new TestTask(randomID());
                    putTask(name,task);
                    flow.withID(randomID()).ioTask(task);
                });
            }
        });

        return this;
    }

    public TestFlowBuilder task(String taskName) {
        TestTask task = new TestTask(randomID());
        putTask(taskName,task);
        rootFlowBuilder.ioTask(task);

        return this;
    }

    public TestFlowBuilder cpuTask(String taskName) {
        TestTask task = new TestTask(randomID());

        putTask(taskName,task);

        rootFlowBuilder.cpuTask(task);

        return this;
    }

    private void putTask(String taskName, TestTask task) {
        if (tasks.put(taskName, task) != null) {
            throw new IllegalArgumentException("Duplicate task name: " + taskName);
        }
    }

    public TestFlow build() {
        return new TestFlow(rootFlowBuilder.build(), ioExecutor, tasks,eventCapturer);
    }

    public static TestFlowBuilder testFlow() {
        return new TestFlowBuilder();
    }
}
