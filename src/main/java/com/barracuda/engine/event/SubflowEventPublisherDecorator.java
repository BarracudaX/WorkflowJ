package com.barracuda.engine.event;

import com.barracuda.engine.event.ExecutionEvent.FlowEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.*;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.*;

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
        if(!(event instanceof FlowEvent flowEvent) || flowEvent.flowID() != subflowID){
            flowEventPublisher.publish(event);
            return;
        }

        switch (flowEvent){
            case FlowStartedEvent _ -> flowEventPublisher.publish(new SubflowStartedEvent(rootID, subflowID));
            case FlowCompletedEvent _ -> flowEventPublisher.publish(new SubflowCompletedEvent(rootID,subflowID));
            case FlowFailedEvent(_, RuntimeException exception) -> flowEventPublisher.publish(new SubflowFailedEvent(rootID, exception, subflowID));
            case FlowPausedEvent _ -> flowEventPublisher.publish(new SubflowPausedEvent(rootID,subflowID));
            case FlowReadyEvent flowReadyEvent -> flowEventPublisher.publish(new SubflowReadyEvent(rootID,subflowID));
            case FlowResetEvent flowResetEvent -> flowEventPublisher.publish(new SubflowResetEvent(rootID,subflowID));
        }
    }

    @Override
    public void subscribe(FlowEventListener eventListener) {
        flowEventPublisher.subscribe(eventListener);
    }
}
