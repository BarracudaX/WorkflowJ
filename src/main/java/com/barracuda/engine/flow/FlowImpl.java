package com.barracuda.engine.flow;

import com.barracuda.engine.chain.ChainNode;

import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.atomic.AtomicReference;

public class FlowImpl implements Flow {

    private final ChainNode chainNode;
    private final AtomicReference<FlowState> state = new AtomicReference<>(FlowState.CREATED);

    public FlowImpl(ChainNode chainNode) {
        this.chainNode = chainNode;
    }

    @Override
    public void execute() {
        if (!state.compareAndSet(FlowState.CREATED, FlowState.RUNNING)) {
            throw new IllegalStateException("Flow cannot be executed because it is in invalid state. Flow state: "+ state.get());
        }

        try (var scope = StructuredTaskScope.open()){
            scope.fork(chainNode::execute);

            scope.join();
        }catch (InterruptedException ex){
            Thread.currentThread().interrupt();
            assert state.get() == FlowState.RUNNING;
            state.compareAndSet(FlowState.RUNNING, FlowState.PAUSED);
            return;
        }catch (StructuredTaskScope.FailedException ex){
            if(ex.getCause() instanceof RuntimeException runtimeException) {
                failed(runtimeException);
            }
            throw ex;
        } catch (Exception e) {
            failed(e);
            throw e;
        }

        state.set(FlowState.COMPLETED);
    }

    private void failed(Exception e) {
        assert state.get() == FlowState.RUNNING;
        state.compareAndSet(FlowState.RUNNING, FlowState.FAILED);

        if(e instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
    }

    @Override
    public FlowState state() {
        return state.get();
    }
}
