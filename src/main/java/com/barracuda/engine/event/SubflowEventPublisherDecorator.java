package com.barracuda.engine.event;

import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowCompletedEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowFailedEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowPausedEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowStartEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.SubflowCompletedEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.SubflowFailedEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.SubflowPausedEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.SubflowStartedEvent;

public class SubflowEventPublisherDecorator implements FlowEventPublisher {

    private final long subflowID;
    private final long rootID;
    private final FlowEventPublisher flowEventPublisher;

    public SubflowEventPublisherDecorator(long subflowID, long rootID, FlowEventPublisher flowEventPublisher) {
        this.subflowID = subflowID;
        this.rootID = rootID;
        this.flowEventPublisher = flowEventPublisher;
    }

    @Override
    public void publish(ExecutionEvent event) {
        switch (event){
            case FlowStartEvent(long flowID) when flowID == subflowID -> flowEventPublisher.publish(new SubflowStartedEvent(rootID, subflowID));
            case FlowCompletedEvent(long flowID) when flowID == subflowID -> flowEventPublisher.publish(new SubflowCompletedEvent(rootID,subflowID));
            case FlowFailedEvent(long flowID, RuntimeException exception) when flowID == subflowID -> flowEventPublisher.publish(new SubflowFailedEvent(rootID,subflowID,exception));
            case FlowPausedEvent(long flowID) when flowID == subflowID -> flowEventPublisher.publish(new SubflowPausedEvent(rootID,subflowID));
            default -> flowEventPublisher.publish(event);
        }
    }

    @Override
    public void subscribe(FlowEventListener eventListener) {
        flowEventPublisher.subscribe(eventListener);
    }
}
