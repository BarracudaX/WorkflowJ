package com.barracuda.engine.flow;

import com.barracuda.engine.chain.ChainNode;
import com.barracuda.engine.chain.ChainNodeImpl;
import com.barracuda.engine.task.Task;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class FlowBuilder<T extends FlowBuilder<T>> {

    protected final List<Function<ChainNode,ChainNode>> chainNodes = new ArrayList<>();

    public <I, R> T step(Task<I, R> task, Supplier<I> inputSupplier, Consumer<R> outputConsumer) {
        chainNodes.add( (next) -> new ChainNodeImpl<>(next,task,inputSupplier,outputConsumer));
        return self();
    }

    protected abstract T self();
}
