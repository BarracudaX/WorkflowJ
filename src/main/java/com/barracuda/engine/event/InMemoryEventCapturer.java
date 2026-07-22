package com.barracuda.engine.event;

import com.barracuda.engine.event.ExecutionEvent.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;

public class InMemoryEventCapturer implements FlowEventListener {

    private final ConcurrentLinkedQueue<ExecutionEvent> events = new ConcurrentLinkedQueue<>();

    @Override
    public void onEvent(ExecutionEvent event) {
        events.add(event);
    }

    public List<ExecutionEvent> events(){
        return events.stream().toList();
    }

    public List<FlowEvent> flowEvents(long flowID){
        List<FlowEvent> flowEvents = new ArrayList<>();

        for(var event : events()){
            if (event instanceof FlowEvent flowEvent && flowEvent.flowID() == flowID) {
                flowEvents.add(flowEvent);
            }
        }

        return flowEvents;
    }

    public List<SubflowEvent> subflowEvents(long rootFlowID, long subflowID) {
        List<SubflowEvent> subflowEvents = new ArrayList<>();

        for(var event : events()){
            if(event instanceof SubflowEvent subflowEvent && subflowEvent.flowID() == rootFlowID && subflowEvent.subflowID() == subflowID) {
                subflowEvents.add(subflowEvent);
            }
        }

        return subflowEvents;
    }

    public List<TaskEvent> taskEvents(long taskID) {
        List<TaskEvent> taskEvents = new ArrayList<>();

        for(var event : events()){
            if (Objects.requireNonNull(event) instanceof TaskEvent taskEvent && taskEvent.taskID() == taskID) {
                taskEvents.add(taskEvent);
            }
        }

        return taskEvents;
    }

}
