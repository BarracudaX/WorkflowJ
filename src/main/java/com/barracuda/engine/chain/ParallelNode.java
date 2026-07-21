package com.barracuda.engine.chain;

import com.barracuda.engine.event.ExecutionEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent;
import com.barracuda.engine.flow.Flow;
import com.barracuda.engine.flow.FlowInterruptedException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ParallelNode implements ChainNode {

    private final Map<Long,Flow> subflows;
    private final ChainNode next;

    public ParallelNode(List<Flow> subflows, ChainNode next) {
        this.subflows = subflows.stream().collect(Collectors.toMap(Flow::id, Function.identity()));
        this.next = next;
    }

    @Override
    public void event(ExecutionEvent event) {

        switch(event){
            case FlowEvent ev when subflows.containsKey(ev.flowID())-> {
                subflows.get(ev.flowID()).event(event);
                return;
            }
            case ExecutionEvent.ContinueEvent _ ->{

            }
            default -> {
                if(next != null){
                    next.event(event);
                }
                return;
            }
        }

        //should get here only if the event was continue
        try (var scope = StructuredTaskScope.open()) {
            subflows.values().forEach(subflow -> scope.fork( () -> subflow.event(event)));
            scope.join();
        } catch (Exception exception) {
            handle(exception);
        }

        if (next != null) {
            next.event(event);
        }
    }

    private void handle(Throwable exception) {
        switch (exception) {
            case InterruptedException ex ->{
                Thread.currentThread().interrupt();
                throw new FlowInterruptedException("Flow interrupted",ex);
            }
            case StructuredTaskScope.FailedException ex -> handle(ex.getCause());
            case RuntimeException ex -> throw ex;
            default -> throw new RuntimeException(exception);
        }
    }

}
