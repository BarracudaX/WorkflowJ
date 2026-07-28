package com.barracuda.engine.flow;

import com.barracuda.engine.command.Command;
import com.barracuda.engine.event.ExecutionEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.*;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.*;

public class SubflowDecorator implements Flow {

    private final Flow subflow;

    public SubflowDecorator(Flow subflow) {
        this.subflow = subflow;
    }


    @Override
    public void command(Command command) {
        subflow.command(command);
    }

    @Override
    public void event(ExecutionEvent event) {
        if(!(event instanceof SubflowEvent subflowEvent) || subflowEvent.subflowID() != subflow.id()) {
            subflow.event(event);
            return;
        }

        switch (subflowEvent) {
            case SubflowStartedEvent(_, long subflowID) -> subflow.event(new FlowStartedEvent(subflowID));
            case SubflowCompletedEvent(_, long subflowID) -> subflow.event(new FlowCompletedEvent(subflowID));
            case SubflowPausedEvent(_,long subflowID) -> subflow.event(new FlowPausedEvent(subflowID));
            case SubflowFailedEvent(_, RuntimeException exception, long subflowID) -> subflow.event(new FlowFailedEvent(subflowID, exception));
            case SubflowResetEvent(_, long subflowID) -> subflow.event(new FlowResetEvent(subflowID));
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
