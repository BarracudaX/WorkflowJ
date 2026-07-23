package com.barracuda.engine.test.builder;

import com.barracuda.engine.builder.AbstractFlowBuilder;
import com.barracuda.engine.builder.FlowBuilder;
import com.barracuda.engine.event.EvenPublisherImpl;
import com.barracuda.engine.event.FlowEventPublisher;
import com.barracuda.engine.event.InMemoryEventCapturer;
import com.barracuda.engine.test.flow.TestFlow;
import com.barracuda.engine.test.flow.TestSubflow;
import com.barracuda.engine.test.task.TestTask;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * Note that tasks are stored in LinkedHashMap for easier testing: tasks are printed in the order in which they were configured by the test.
 */
public class TestFlowBuilder {

    private final ExecutorService cpuExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private final ExecutorService ioExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final InMemoryEventCapturer eventCapturer = new InMemoryEventCapturer();
    private final FlowEventPublisher evenPublisher = new EvenPublisherImpl();
    private final FlowBuilder flowBuilder = new FlowBuilder(1L, cpuExecutor, ioExecutor,evenPublisher);
    private final Map<Class<?>, Map<String, TestTask<?>>> testTasks = new LinkedHashMap<>();
    private final Map<String,Long> subflowsMap = new LinkedHashMap<>();
    private long nextID = 2;

    private TestFlowBuilder() {
        evenPublisher.subscribe(eventCapturer);
        testTasks.put(Void.class,new LinkedHashMap<>()); // create entry for Void input by default
    }

    public <T> TestFlowBuilder consumerTask(String taskName, Class<T> clazz, Supplier<T> dataSupplier) {

        testTasks.putIfAbsent(clazz, new LinkedHashMap<>());

        createAndSaveTask(taskName,flowBuilder,clazz,dataSupplier);

        return this;
    }

    public TestFlowBuilder subflows(TestSubflow... testSubflows) {
        flowBuilder.parallel(parallel -> {
            for(TestSubflow testSubflow : testSubflows) {

                var subflowID = nextID++;

                parallel.subflow(subflowID,subflow -> {

                    for (String taskName : testSubflow.tasks()) {
                        createAndSaveTask(taskName,subflow,Void.class,FlowBuilder.nullSupplier());
                    }

                    if(subflowsMap.put(testSubflow.name(),subflowID) != null){
                        throw new IllegalArgumentException("Duplicate subflow name: " + testSubflow.name());
                    }

                });
            }
        });
        return this;
    }

    private <I> void createAndSaveTask(String taskName, AbstractFlowBuilder<?> builder, Class<I> clazz, Supplier<I> dataSupplier) {
        var task = new TestTask<I>(nextID++,taskName);

        builder.ioTask(task,dataSupplier);

        saveTask(taskName, task, clazz);

    }

    public TestFlowBuilder task(String taskName) {
        TestTask<Void> task = new TestTask<>(nextID++,taskName);
        saveTask(taskName,task, Void.class);
        flowBuilder.ioTask(task);

        return this;
    }

    public TestFlowBuilder cpuTask(String taskName) {
        TestTask<Void> task = new TestTask<>(nextID++,taskName);

        saveTask(taskName,task, Void.class);

        flowBuilder.cpuTask(task);

        return this;
    }

    private <I> void saveTask(String taskName, TestTask<I> task, Class<I> clazz) {
        if (testTasks.get(clazz).put(taskName, task) != null) {
            throw new IllegalArgumentException("Duplicate task name: " + taskName);
        }
    }

    public TestFlow build() {
        return new TestFlow(flowBuilder.build(), ioExecutor, testTasks ,eventCapturer, subflowsMap);
    }

    public static TestFlowBuilder testFlow() {
        return new TestFlowBuilder();
    }
}
