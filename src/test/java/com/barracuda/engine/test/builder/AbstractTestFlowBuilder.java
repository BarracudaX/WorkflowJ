package com.barracuda.engine.test.builder;

import com.barracuda.engine.builder.AbstractFlowBuilder;
import com.barracuda.engine.builder.FlowBuilder;
import com.barracuda.engine.event.EvenPublisherImpl;
import com.barracuda.engine.event.FlowEventPublisher;
import com.barracuda.engine.event.InMemoryEventCapturer;
import com.barracuda.engine.test.flow.TestSubflow;
import com.barracuda.engine.test.task.TestTask;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Note that tasks are stored in LinkedHashMap for easier testing: tasks are printed in the order in which they were configured by the test.
 */
public abstract class AbstractTestFlowBuilder<T extends AbstractTestFlowBuilder<T>>{

    protected final InMemoryEventCapturer eventCapturer = new InMemoryEventCapturer();
    protected final FlowEventPublisher evenPublisher = new EvenPublisherImpl();
    protected final FlowBuilder flowBuilder = new FlowBuilder(1L, Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()), Executors.newVirtualThreadPerTaskExecutor(),evenPublisher);
    protected final Map<Class<?>, Map<String, TestTask<?>>> testTasks = new LinkedHashMap<>();
    protected final Map<String,Long> subflowsMap = new LinkedHashMap<>();
    protected final AtomicLong counter = new AtomicLong(0); // the only reason we are using atomic is for easier sharing(not thread safety).

    AbstractTestFlowBuilder() {
        evenPublisher.subscribe(eventCapturer);
        testTasks.put(Void.class,new LinkedHashMap<>()); // create entry for Void input by default
    }

    public <I> T consumerTask(String taskName, Class<I> clazz, Supplier<I> dataSupplier) {

        testTasks.putIfAbsent(clazz, new LinkedHashMap<>());

        createAndSaveTask(taskName,flowBuilder,clazz,dataSupplier);

        return self();
    }

    public T subflows(TestSubflow... testSubflows) {
        flowBuilder.parallel(parallel -> {
            for(TestSubflow testSubflow : testSubflows) {
                var subflowID = counter.incrementAndGet();

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
        return self();
    }

    public T ioTask(String taskName) {
        TestTask<Void> task = new TestTask<>(counter.incrementAndGet(),taskName);
        saveTask(taskName,task, Void.class);
        flowBuilder.ioTask(task);

        return self();
    }

    public T cpuTask(String taskName) {
        TestTask<Void> task = new TestTask<>(counter.incrementAndGet(),taskName);

        saveTask(taskName,task, Void.class);

        flowBuilder.cpuTask(task);

        return self();
    }

    private <I> void createAndSaveTask(String taskName, AbstractFlowBuilder<?> builder, Class<I> clazz, Supplier<I> dataSupplier) {
        var task = new TestTask<I>(counter.incrementAndGet(),taskName);

        builder.ioTask(task,dataSupplier);

        saveTask(taskName, task, clazz);
    }

    private <I> void saveTask(String taskName, TestTask<I> task, Class<I> clazz) {
        if (testTasks.get(clazz).put(taskName, task) != null) {
            throw new IllegalArgumentException("Duplicate task name: " + taskName);
        }
    }

    protected abstract T self();

}
