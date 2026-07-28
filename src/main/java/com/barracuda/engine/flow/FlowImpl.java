package com.barracuda.engine.flow;

import com.barracuda.engine.chain.ChainNode;
import com.barracuda.engine.command.Command;
import com.barracuda.engine.command.Command.Continue;
import com.barracuda.engine.command.Command.Reset;
import com.barracuda.engine.command.CommandRejectedException;
import com.barracuda.engine.event.ExecutionEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.*;
import com.barracuda.engine.event.FlowEventPublisher;

import java.util.Objects;
import java.util.concurrent.StructuredTaskScope;

public class FlowImpl implements Flow {

    private final ChainNode nextNode;
    private volatile FlowStatus status = FlowStatus.READY_TO_CONTINUE;
    private final long flowID;
    private final FlowContext context;
    private final FlowEventPublisher flowEventPublisher;

    public FlowImpl(ChainNode nextNode, FlowContext context, long flowID) {
        this.context = Objects.requireNonNull(context);
        this.nextNode = Objects.requireNonNull(nextNode);
        this.flowID = flowID;
        this.flowEventPublisher = context.flowEventPublisher();
    }

    @Override
    public void command(Command command) {
        switch (command) {
            case Continue continueCommand -> handleContinueCommand(continueCommand);
            case Reset resetCommand -> handleResetCommand(resetCommand);
        }
    }

    private void handleResetCommand(Reset reset) {
        if (status == FlowStatus.RUNNING) {
            throw new CommandRejectedException("Cannot reset flow that is currently running.");
        }

        propagateCommand(reset);

        flowReset();
    }

    private void flowReset() {
        var flowResetEvent = new FlowResetEvent(flowID);

        flowEventPublisher.publish(flowResetEvent);

        event(flowResetEvent);

    }

    private void handleContinueCommand(Continue continueCommand) {
        if(status == FlowStatus.COMPLETED) {
            propagateCommand(continueCommand);
            return;
        }

        if (status != FlowStatus.READY_TO_CONTINUE) {
            throw new  CommandRejectedException("Cannot continue flow because of its current state being "+status);
        }

        flowStarted();

        propagateCommand(continueCommand);

        flowCompleted();
    }

    private void flowCompleted() {
        FlowCompletedEvent flowCompletedEvent = new FlowCompletedEvent(flowID);
        flowEventPublisher.publish(flowCompletedEvent);

        event(flowCompletedEvent);
    }

    private void flowStarted() {
        var flowStartedEvent = new FlowStartedEvent(flowID);

        flowEventPublisher.publish(flowStartedEvent);

        event(flowStartedEvent);
    }

    private void propagateCommand(Command command) {
        ScopedValue.where(FLOW_CONTEXT, context).run(() -> {

            try (var scope = StructuredTaskScope.open()) {
                scope.fork(() -> nextNode.command(command));

                scope.join();
            } catch (Exception e) {
                handle(e);
            }

        });
    }

    @Override
    public void event(ExecutionEvent event) {
        if (Objects.requireNonNull(event) instanceof FlowEvent ev && ev.flowID() == flowID) {
            flowEvent(ev);
        } else {
            propagateEvent(event);
        }
    }

    private void flowEvent(FlowEvent flowEvent) {
        switch (flowEvent) {
            case FlowStartedEvent _ -> flowStartedEvent();
            case FlowCompletedEvent _ -> flowCompletedEvent(); // already completed.
            case FlowFailedEvent ev -> flowFailedEvent(ev);
            case FlowResetEvent _ -> flowResetEvent();
            case FlowReadyEvent _ -> flowReadyEvent();
            case FlowPausedEvent _ -> flowPausedEvent();
        }
    }


    private void flowPausedEvent() {
        status = FlowStatus.PAUSED;
    }

    private void flowReadyEvent() {
        status = FlowStatus.READY_TO_CONTINUE;
    }

    private void flowFailedEvent(FlowFailedEvent flowFailedEvent) {
        status = FlowStatus.FAILED;
        throw flowFailedEvent.exception();
    }

    private void flowCompletedEvent() {
        status = FlowStatus.COMPLETED;
    }

    private void flowResetEvent() {
        status = FlowStatus.READY_TO_CONTINUE;

    }

    private void flowStartedEvent() {
        status = FlowStatus.RUNNING;
    }

    private void propagateEvent(ExecutionEvent event) {
        ScopedValue.where(FLOW_CONTEXT, context).run(() -> {

            try (var scope = StructuredTaskScope.open()) {
                scope.fork(() -> nextNode.event(event));

                scope.join();
            } catch (Exception e) {
                handle(e);
            }

        });
    }

    private void handle(Throwable exception) {
        switch (exception) {
            case FlowInterruptedException ex-> {
                FlowPausedEvent flowPausedEvent = new FlowPausedEvent(flowID);
                flowEventPublisher.publish(flowPausedEvent);
                event(flowPausedEvent);
                throw ex;
            }
            case InterruptedException ex ->handle(new FlowInterruptedException("Flow Interrupted",ex));
            case StructuredTaskScope.FailedException ex -> handle(ex.getCause());
            case RuntimeException ex -> {
                FlowFailedEvent flowFailedEvent = new FlowFailedEvent(flowID, ex);
                flowEventPublisher.publish(flowFailedEvent);
                event(flowFailedEvent);
            }
            default -> handle(new RuntimeException(exception));
        }
    }

    @Override
    public FlowStatus status() {
        return status;
    }

    @Override
    public long id() {
        return flowID;
    }

    @Override
    public void prettyPrint(FlowPrettyOutput output) {
        output.increaseLevel();

        StringBuilder sb = output.getStringBuilder();

        sb.append("\n").append(output.getTab()).append("[Flow]");

        sb.append("\n").append(output.getTab()).append("Status:").append(status);

        sb.append("\n").append(output.getTab()).append("Next Node:");
        nextNode.prettyPrint(output);

        output.decreaseLevel();
    }
}
