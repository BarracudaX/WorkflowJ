package com.barracuda.engine.test.builder;

import com.barracuda.engine.builder.AbstractFlowBuilder;
import com.barracuda.engine.builder.FlowBuilder;
import com.barracuda.engine.event.EvenPublisherImpl;
import com.barracuda.engine.event.FlowEventPublisher;
import com.barracuda.engine.event.InMemoryEventCapturer;
import com.barracuda.engine.test.flow.TestFlow;
import com.barracuda.engine.test.task.TestTask;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Note that tasks are stored in LinkedHashMap for easier testing: tasks are printed in the order in which they were configured by the test.
 */
public class TestFlowBuilder{

    protected final InMemoryEventCapturer eventCapturer;
    protected final Map<Class<?>, Map<String, TestTask<?>>> testTasks = new LinkedHashMap<>();
    protected final Map<String, TestFlow> subflows = new LinkedHashMap<>();
    private final ExecutorService cpuExecutor;
    private final ExecutorService ioExecutor;
    protected final FlowBuilder builder;
    protected final AtomicLong counter; // the only reason we are using atomic is for easier sharing(not thread safety).
    protected final long flowID;
    private final String name;

    /**
     * Use this constructor for subflows
     */
    TestFlowBuilder(FlowBuilder builder,ExecutorService cpuExecutor,long flowID, ExecutorService ioExecutor,InMemoryEventCapturer eventCapturer,AtomicLong counter,String name) {
        this.flowID = flowID;
        this.name = name;
        this.cpuExecutor = cpuExecutor;
        this.ioExecutor = ioExecutor;
        this.builder = builder;
        this.eventCapturer = eventCapturer;
        this.counter = counter;
        testTasks.put(Void.class,new LinkedHashMap<>()); // create entry for Void input by default
    }

    /**
     * Use this constructor for root TestFlow
     */
    TestFlowBuilder() {
        var eventCapturer = new InMemoryEventCapturer();
        var cpuExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        var ioExecutor = Executors.newVirtualThreadPerTaskExecutor();
        var builder = new FlowBuilder(1L, cpuExecutor, ioExecutor);

        FlowEventPublisher eventPublisher = new EvenPublisherImpl();
        eventPublisher.subscribe(eventCapturer);
        builder.eventPublisher(eventPublisher);

        this(builder,cpuExecutor, 1,ioExecutor,eventCapturer,new AtomicLong(1),null);
    }

    public TestFlowBuilder parallel(Consumer<TestSubflowBuilder> consumer) {
        builder.parallel(subflowBuilder -> {
            var testSubflowBuilder = new TestSubflowBuilder(subflowBuilder);
            consumer.accept(testSubflowBuilder);
        });

        return this;
    }

    public <I> TestFlowBuilder consumerTask(String taskName, Class<I> clazz, Supplier<I> dataSupplier) {

        testTasks.putIfAbsent(clazz, new LinkedHashMap<>());

        createAndSaveTask(taskName,builder,clazz,dataSupplier);

        return this;
    }

    public TestFlowBuilder ioTask(String taskName) {
        TestTask<Void> task = new TestTask<>(counter.incrementAndGet(),taskName);
        saveTask(taskName,task, Void.class);
        builder.ioTask(task);

        return this;
    }

    public TestFlowBuilder cpuTask(String taskName) {
        TestTask<Void> task = new TestTask<>(counter.incrementAndGet(),taskName);

        saveTask(taskName,task, Void.class);

        builder.cpuTask(task);

        return this;
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

    public class TestSubflowBuilder {

        private final AbstractFlowBuilder<?>.SubflowBuilder parallelFlowBuilder;

        public TestSubflowBuilder(AbstractFlowBuilder<FlowBuilder>.SubflowBuilder parallelFlowBuilder) {
            this.parallelFlowBuilder = parallelFlowBuilder;
        }

        public TestSubflowBuilder subflow(String subflowName, Consumer<TestFlowBuilder> consumer) {
            var subflowID = counter.incrementAndGet();
            parallelFlowBuilder.subflow(subflowID,(builder) -> {
                var newBuilder = new TestFlowBuilder((FlowBuilder) builder,cpuExecutor, subflowID, ioExecutor,eventCapturer,counter,subflowName);
                consumer.accept(newBuilder);
                subflows.put(subflowName, newBuilder.build());
            });
            return this;
        }



    }

    public static TestFlowBuilder testFlow() {
        return new TestFlowBuilder();
    }

    public TestFlow build(){
        return new TestFlow(eventCapturer, builder.build(), subflows, testTasks);
    }
}
