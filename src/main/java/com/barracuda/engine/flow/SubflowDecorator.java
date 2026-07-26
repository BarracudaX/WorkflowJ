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
            case SubflowStartedEvent(_, long subflowID) when subflowID == subflow.id() -> subflow.event(new FlowStartedEvent(subflowID));
            case SubflowCompletedEvent(_, long subflowID) when subflowID == subflow.id() -> subflow.event(new FlowCompletedEvent(subflowID));
            case SubflowPausedEvent(_,long subflowID) when subflowID == subflow.id() -> subflow.event(new FlowPausedEvent(subflowID));
            case SubflowFailedEvent(_, RuntimeException exception, long subflowID) when subflowID == subflow.id() -> subflow.event(new FlowFailedEvent(subflowID, exception));
            default -> subflow.event(event);
        }
    }

    @Override
    public void prettyPrint(FlowPrettyOutput output) {
        subflow.prettyPrint(output);
    }

    @Override
    public FlowStatus status() {
        return subflow.status();
    }

    @Override
    public long id() {
        return subflow.id();
    }
}
