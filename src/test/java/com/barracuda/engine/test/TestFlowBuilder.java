package com.barracuda.engine.test;

import com.barracuda.engine.builder.RootFlowBuilder;
import com.barracuda.engine.event.EvenPublisherImpl;
import com.barracuda.engine.event.FlowEventPublisher;
import com.barracuda.engine.event.InMemoryEventCapturer;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import static com.barracuda.engine.utility.TestUtils.randomID;

/**
 * Note that tasks are stored in LinkedHashMap for easier testing: tasks are printed in the order in which they were configured by the test.
 */
public class TestFlowBuilder {

    private final ExecutorService cpuExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private final ExecutorService ioExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final InMemoryEventCapturer eventCapturer = new InMemoryEventCapturer();
    private final FlowEventPublisher evenPublisher = new EvenPublisherImpl();
    private final RootFlowBuilder rootFlowBuilder = new RootFlowBuilder(cpuExecutor, ioExecutor,evenPublisher).withID(randomID());
    private final Map<Class<?>, Map<String,TestTask<?>>> testTasks = new LinkedHashMap<>();
    private final Map<String,Long> subflowsMap = new LinkedHashMap<>();

    private TestFlowBuilder() {
        evenPublisher.subscribe(eventCapturer);
        testTasks.put(Void.class,new LinkedHashMap<>()); // create entry for Void input by default
    }

    public <T> TestFlowBuilder consumerTask(String taskName, Class<T> clazz, Supplier<T> dataSupplier) {
        testTasks.putIfAbsent(clazz, new LinkedHashMap<>());
        TestTask<T> task = new TestTask<>(randomID());
        if (testTasks.get(clazz).put(taskName, task) != null) {
            throw new IllegalArgumentException("Duplicate task name: " + taskName);
        }
        rootFlowBuilder.ioTask(task,dataSupplier);
        return this;
    }

    public TestFlowBuilder subflows(TestSubflow... testSubflows) {
        rootFlowBuilder.parallel(parallel -> {
            for(TestSubflow testSubflow : testSubflows) {
                var flowID = randomID();
                parallel.subflow(subflow -> {
                    subflow.withID(flowID);
                    for (String taskName : testSubflow.tasks()) {
                        var task = new TestTask<Void>(randomID());
                        subflow.ioTask(task);
                        putTask(taskName, task);
                    }
                    if(subflowsMap.put(testSubflow.name(),flowID) != null){
                        throw new IllegalArgumentException("Duplicate subflow name: " + testSubflow.name());
                    }
                });
            }
        });
        return this;
    }

    public TestFlowBuilder task(String taskName) {
        TestTask<Void> task = new TestTask<>(randomID());
        putTask(taskName,task);
        rootFlowBuilder.ioTask(task);

        return this;
    }

    public TestFlowBuilder cpuTask(String taskName) {
        TestTask<Void> task = new TestTask<>(randomID());

        putTask(taskName,task);

        rootFlowBuilder.cpuTask(task);

        return this;
    }

    private void putTask(String taskName, TestTask<Void> task) {
        if (testTasks.get(Void.class).put(taskName, task) != null) {
            throw new IllegalArgumentException("Duplicate task name: " + taskName);
        }
    }

    public TestFlow build() {
        return new TestFlow(rootFlowBuilder.build(), ioExecutor, testTasks ,eventCapturer, subflowsMap);
    }

    public static TestFlowBuilder testFlow() {
        return new TestFlowBuilder();
    }
}
