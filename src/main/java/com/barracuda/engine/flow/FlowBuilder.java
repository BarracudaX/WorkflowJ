package com.barracuda.engine.flow;

import com.barracuda.engine.chain.ChainNode;
import com.barracuda.engine.chain.TaskChainNode;
import com.barracuda.engine.task.Task;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class FlowBuilder<T extends FlowBuilder<T>> {

    protected final List<Function<ChainNode,ChainNode>> chainNodes = new ArrayList<>();

    public <I, R> T step(Task<I, R> task, Supplier<I> inputSupplier, Consumer<R> outputConsumer) {
        chainNodes.add( (next) -> new TaskChainNode<>(next,task,inputSupplier,outputConsumer));
        return self();
    }

    /**
     * Configures a task with default input provider(provides null) and default output consumer(does nothing with the result).
     * This method is useful for tasks that have no input and produce no output. For example, for test tasks or for tasks that do execute deterministic code(do not rely on any input) with side effects.
     * This method shouldn't be used unless absolutely needed. Task should be pure function that depend on their input and produce output.
     * @param task the configured task
     * @return this builder
     */
    public T step(Task<Void, Void> task) {
        chainNodes.add((next) -> new TaskChainNode<>(next,task,provideInput(),doNothingWithOutput()));
        return self();
    }

    private static <T> Consumer<T> doNothingWithOutput(){
        return _ -> {};
    }

    private static <T>Supplier<T> provideInput(){
        return () -> (T) null;
    }

    protected abstract T self();
}
