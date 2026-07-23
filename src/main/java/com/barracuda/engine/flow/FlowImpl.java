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

/**
 *
 * Flow instances aren't thread safe and aren't supposed to be shared between used. The thread safety of a flow is achieved through thread confinement. Despite that, the class guarantees visibility of changes.
 */
public class FlowImpl implements Flow {

    private final ChainNode chainNode;
    private volatile FlowState state = FlowState.READY;
    private final long flowID;
    private final FlowContext context;
    private volatile boolean startedEventPublished = false;

    public FlowImpl(ChainNode chainNode, FlowContext context,long flowID) {
        this.context = Objects.requireNonNull(context);
        this.chainNode = chainNode;
        this.flowID = flowID;
    }

    @Override
    public void event(ExecutionEvent event) {
        if (state == FlowState.FAILED || state == FlowState.COMPLETED || state == FlowState.RUNNING) {
            throw new IllegalStateException("Flow cannot accept events due to its state: "+state+".");
        }

        switch (event){
            case FlowStartedEvent _ -> {
                if(startedEventPublished) {
                    throw new IllegalStateException("Duplicate flow started event.");
                }
                startedEventPublished = true;
                if (state != FlowState.READY) {
                    throw new IllegalStateException("Flow cannot be started because of it's current state being "+state);
                }
            }
            case FlowCompletedEvent _ -> state = FlowState.COMPLETED; // already completed.
            case FlowFailedEvent ev -> {
                state = FlowState.FAILED;
                throw ev.exception();
            }
            case FlowPausedEvent _ -> state = FlowState.PAUSED;
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
        if(!startedEventPublished) {
            context.flowEventPublisher().publish(new FlowStartedEvent(flowID));
            startedEventPublished = true;
            state = FlowState.RUNNING;
        }

        if (state != FlowState.RUNNING) {
            throw new IllegalStateException("Cannot continue a flow that's in "+state+" state.");
        }

        propagateEvent(continueEvent);

        context.flowEventPublisher().publish(new FlowCompletedEvent(flowID));
        state = FlowState.COMPLETED;
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
        assert state == FlowState.RUNNING;

        if(state != FlowState.RUNNING) {
            throw new IllegalStateException("Cannot interrupt a flow that's in "+state+" state.");
        }

        state = FlowState.PAUSED;
        context.flowEventPublisher().publish(new FlowPausedEvent(flowID));
        throw ex;
    }

    private void failed(RuntimeException ex) {
        assert state == FlowState.RUNNING;

        if(state != FlowState.RUNNING) {
            throw new IllegalStateException("Not running flow failed with exception",ex);
        }
        state = FlowState.FAILED;
        context.flowEventPublisher().publish(new FlowFailedEvent(flowID,ex));

        throw ex;
    }

    @Override
    public FlowState state() {
        return state;
    }

    @Override
    public long id() {
        return flowID;
    }
}
