package com.barracuda.engine.flow;

import com.barracuda.engine.event.ExecutionEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowCompletedEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowFailedEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowPausedEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowStartedEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.SubflowCompletedEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.SubflowFailedEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.SubflowPausedEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.SubflowStartedEvent;

public class SubflowDecorator implements Flow {

    private final Flow subflow;

    public SubflowDecorator(Flow subflow) {
        this.subflow = subflow;
    }


    @Override
    public void event(ExecutionEvent event) {
        switch (event){
            case SubflowStartedEvent(_, long subflowID) -> subflow.event(new FlowStartedEvent(subflowID));
            case SubflowCompletedEvent(_, long subflowID) -> subflow.event(new FlowCompletedEvent(subflowID));
            case SubflowPausedEvent(_,long subflowID) -> subflow.event(new FlowPausedEvent(subflowID));
            case SubflowFailedEvent(_, long subflowID, RuntimeException exception) -> subflow.event(new FlowFailedEvent(subflowID, exception));
            default -> subflow.event(event);
        }
    }

    @Override
    public FlowState state() {
        return subflow.state();
    }

    @Override
    public long id() {
        return subflow.id();
    }
}
