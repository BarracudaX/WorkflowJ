package com.barracuda.engine.chain;

import com.barracuda.engine.command.Command;
import com.barracuda.engine.event.ExecutionEvent;
import com.barracuda.engine.flow.Flow;
import com.barracuda.engine.flow.FlowInterruptedException;
import com.barracuda.engine.flow.FlowPrettyOutput;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;


public class ParallelNode implements ChainNode {

    private final Map<Long,Flow> subflows;
    private final ChainNode next;

    public ParallelNode(List<Flow> subflows, ChainNode next) {
        this.subflows = subflows.stream().collect(Collectors.toMap(Flow::id, Function.identity()));
        this.next = Objects.requireNonNull(next);
    }


    @Override
    public void command(Command command) {
        propagateCommand(command);
    }

    private void propagateCommand(Command command){
        try (var scope = StructuredTaskScope.open()) {
            subflows.values().forEach(subflow -> scope.fork( () -> subflow.command(command)));
            scope.join();
        } catch (Exception exception) {
            handle(exception);
        }

        next.command(command);
    }

    @Override
    public void event(ExecutionEvent event) {
        try (var scope = StructuredTaskScope.open()) {
            subflows.values().forEach(subflow -> scope.fork( () -> subflow.event(event)));
            scope.join();
        } catch (Exception exception) {
            handle(exception);
        }

        next.event(event);
    }

    @Override
    public void prettyPrint(FlowPrettyOutput output) {
        StringBuilder sb = output.getStringBuilder();
        sb
                .append("\n").append(output.getTab()).append("[Parallel Node]")
                .append("\n").append(output.getTab()).append("Subflows:");

        subflows.values().forEach(subflow -> subflow.prettyPrint(output));

        next.prettyPrint(output);
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
