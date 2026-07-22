package com.barracuda.engine.flow;

import com.barracuda.engine.event.ExecutionEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowCompletedEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowFailedEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowPausedEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowStartEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.SubflowCompletedEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.SubflowFailedEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.SubflowPausedEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.SubflowStartedEvent;

public class SubflowDecorator implements Flow {

    private final Flow flow;

    public SubflowDecorator(Flow flow) {
        this.flow = flow;
    }


    @Override
    public void event(ExecutionEvent event) {
        switch (event){
            case SubflowStartedEvent(_, long subflowID) -> flow.event(new FlowStartEvent(subflowID));
            case SubflowCompletedEvent(_, long subflowID) -> flow.event(new FlowCompletedEvent(subflowID));
            case SubflowPausedEvent(_,long subflowID) -> flow.event(new FlowPausedEvent(subflowID));
            case SubflowFailedEvent(_, long subflowID, RuntimeException exception) -> flow.event(new FlowFailedEvent(subflowID, exception));
            default -> flow.event(event);
        }
    }

    @Override
    public FlowState state() {
        return flow.state();
    }

    @Override
    public long id() {
        return flow.id();
    }
}
