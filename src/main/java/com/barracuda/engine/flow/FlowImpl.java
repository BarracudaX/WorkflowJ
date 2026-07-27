package com.barracuda.engine.flow;

import com.barracuda.engine.chain.ChainNode;
import com.barracuda.engine.event.Command;
import com.barracuda.engine.event.Command.Continue;
import com.barracuda.engine.event.Command.Prepare;
import com.barracuda.engine.event.Command.Reset;
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
    private volatile boolean startedEventPublished = false;
    private final FlowEventPublisher flowEventPublisher;

    public FlowImpl(ChainNode nextNode, FlowContext context, long flowID) {
        this.context = Objects.requireNonNull(context);
        this.nextNode = nextNode;
        this.flowID = flowID;
        this.flowEventPublisher = context.flowEventPublisher();
    }

    @Override
    public void command(Command command) {
        switch (command) {
            case Continue continueCommand -> handleContinueCommand(continueCommand);
            case Prepare prepareCommand -> handlePrepareCommand(prepareCommand);
            case Reset resetCommand -> handleResetCommand(resetCommand);
        }
    }

    private void handleResetCommand(Reset reset) {
        if (status == FlowStatus.RUNNING) {
            throw new IllegalStateException("Cannot reset flow that is currently running.");
        }

        propagateCommand(reset);

        var flowResetEvent = new FlowResetEvent(flowID);
        flowEventPublisher.publish(flowResetEvent);
        event(flowResetEvent);

    }

    private void handlePrepareCommand(Prepare prepare) {
        if (status != FlowStatus.REPLAY_MODE) {
            throw new IllegalStateException("Cannot prepare flow because of its current status " + status);
        }

        propagateCommand(prepare);

        FlowReadyEvent flowReadyEvent = new FlowReadyEvent(flowID);
        flowEventPublisher.publish(flowReadyEvent);

        event(flowReadyEvent);
    }

    private void handleContinueCommand(Continue continueCommand) {
        if (status != FlowStatus.READY_TO_CONTINUE) {
            throw new IllegalStateException("Cannot continue a flow that's in " + status + " state.");
        }

        if (!startedEventPublished) {
            var flowStartedEvent = new FlowStartedEvent(flowID);
            flowEventPublisher.publish(flowStartedEvent);
            event(flowStartedEvent);
        }

        propagateCommand(continueCommand);

        FlowCompletedEvent flowCompletedEvent = new FlowCompletedEvent(flowID);
        flowEventPublisher.publish(flowCompletedEvent);

        event(flowCompletedEvent);
    }

    private void propagateCommand(Command command) {
        if(nextNode == null){
            return;
        }

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
            case FlowStartedEvent ev -> flowStartedEvent(ev);
            case FlowCompletedEvent ev -> flowCompletedEvent(ev); // already completed.
            case FlowFailedEvent ev -> flowFailedEvent(ev);
            case FlowResetEvent ev -> flowResetEvent(ev);
            case FlowReadyEvent ev -> flowReadyEvent(ev);
            case FlowPausedEvent ev -> flowPausedEvent(ev);
        }
    }


    private void flowPausedEvent(FlowPausedEvent flowPausedEvent) {
        status = FlowStatus.PAUSED;
    }

    private void flowReadyEvent(FlowReadyEvent flowReadyEvent) {
        status = FlowStatus.READY_TO_CONTINUE;
    }

    private void flowFailedEvent(FlowFailedEvent flowFailedEvent) {
        status = FlowStatus.FAILED;
        throw flowFailedEvent.exception();
    }

    private void flowCompletedEvent(FlowCompletedEvent flowCompletedEvent) {
        status = FlowStatus.COMPLETED;
    }

    private void flowResetEvent(FlowResetEvent flowResetEvent) {
        status = FlowStatus.READY_TO_CONTINUE;

        startedEventPublished = false;
    }

    private void flowStartedEvent(FlowStartedEvent startedEvent) {
        status = FlowStatus.RUNNING;
    }

    private void propagateEvent(ExecutionEvent event) {

        if (nextNode == null) {
            return;
        }

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

        if (nextNode != null) {
            sb.append("\n").append(output.getTab()).append("Next Node:");
            nextNode.prettyPrint(output);
        }

        output.decreaseLevel();
    }
}
