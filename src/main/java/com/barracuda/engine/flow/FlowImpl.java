package com.barracuda.engine.flow;

import com.barracuda.engine.chain.ChainNode;
import com.barracuda.engine.event.ExecutionEvent;
import com.barracuda.engine.event.ExecutionEvent.CommandEvent;
import com.barracuda.engine.event.ExecutionEvent.CommandEvent.Continue;
import com.barracuda.engine.event.ExecutionEvent.CommandEvent.Reset;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowCompletedEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowFailedEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowPausedEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowStartedEvent;

import java.util.Objects;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class FlowImpl implements Flow {


    private final ChainNode chainNode;
    private final AtomicReference<FlowState> state = new AtomicReference<>(FlowState.READY);
    private final long flowID;
    private final FlowContext context;
    private final AtomicBoolean havePublishedStartEvent = new AtomicBoolean(false);

    public FlowImpl(ChainNode chainNode, FlowContext context,long flowID) {
        this.context = Objects.requireNonNull(context);
        this.chainNode = chainNode;
        this.flowID = flowID;
    }

    @Override
    public void event(ExecutionEvent event) {
        switch (event){
            case FlowStartedEvent _ -> {
                if(!havePublishedStartEvent.compareAndSet(false, true)) {
                    throw new IllegalStateException("Flow potentially getting events from multiple threads");
                }
            }
            case FlowCompletedEvent _ -> state.set(FlowState.COMPLETED); // already completed.
            case FlowFailedEvent ev -> {
                state.set(FlowState.FAILED);
                throw ev.exception();
            }
            case FlowPausedEvent _ -> state.set(FlowState.PAUSED);
            case CommandEvent command -> handleCommand(command);
            default -> propagateEvent(event);
        }
    }

    private void handleCommand(CommandEvent event) {
        switch (event) {
            case Continue ev -> handleContinueCommand(ev);
            case Reset ev -> { }
        }
    }

    private void handleContinueCommand(Continue continueEvent) {
        if (!state.compareAndSet(FlowState.READY, FlowState.RUNNING)) {
            throw new IllegalStateException("Flow cannot run because it is in invalid state. Flow state: "+ state.get());
        }

        if(havePublishedStartEvent.compareAndSet(false, true)) {
            context.flowEventPublisher().publish(new FlowStartedEvent(flowID));
        }

        propagateEvent(continueEvent);

        context.flowEventPublisher().publish(new FlowCompletedEvent(flowID));
        state.set(FlowState.COMPLETED);
    }

    private void propagateEvent(ExecutionEvent event){

        if(chainNode == null) {
            return;
        }

        ScopedValue.where(FLOW_CONTEXT, context).run(() -> {
            try (var scope = StructuredTaskScope.open()){
                scope.fork(() -> chainNode.event(event));

                scope.join();
            } catch (Exception e) {
                handle(e);
            }
        });
    }

    private void handle(Throwable exception){
        switch (exception) {
            case FlowInterruptedException ex -> interrupted(ex);
            case InterruptedException ex -> interrupted(new FlowInterruptedException("Flow Interrupted",ex));
            case StructuredTaskScope.FailedException ex -> handle(ex.getCause());
            case RuntimeException ex -> failed(ex);
            default -> failed(new RuntimeException(exception));
        }
    }

    private void interrupted(FlowInterruptedException ex){
        Thread.currentThread().interrupt();
        assert state.get() == FlowState.RUNNING;
        state.compareAndSet(FlowState.RUNNING, FlowState.PAUSED);
        context.flowEventPublisher().publish(new FlowPausedEvent(flowID));
        throw ex;
    }

    private void failed(RuntimeException ex) {
        assert state.get() == FlowState.RUNNING;
        state.compareAndSet(FlowState.RUNNING, FlowState.FAILED);
        context.flowEventPublisher().publish(new FlowFailedEvent(flowID,ex));

        throw ex;
    }

    @Override
    public FlowState state() {
        return state.get();
    }

    @Override
    public long id() {
        return flowID;
    }
}
