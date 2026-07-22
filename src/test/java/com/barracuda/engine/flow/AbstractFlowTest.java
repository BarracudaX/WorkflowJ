package com.barracuda.engine.flow;

import com.barracuda.engine.builder.FlowBuilder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class AbstractFlowTest {

    protected final ExecutorService cpuTaskExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    protected final ExecutorService ioTaskExecutor = Executors.newVirtualThreadPerTaskExecutor();
    protected final FlowBuilder flowBuilder = new FlowBuilder(1L, cpuTaskExecutor, ioTaskExecutor);

}
