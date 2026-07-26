package com.barracuda.engine.flow;

import com.barracuda.engine.chain.ChainNode;
import com.barracuda.engine.event.ExecutionEvent;
import com.barracuda.engine.event.ExecutionEvent.CommandEvent;
import com.barracuda.engine.event.ExecutionEvent.CommandEvent.Continue;
import com.barracuda.engine.event.ExecutionEvent.CommandEvent.EnterReplayMode;
import com.barracuda.engine.event.ExecutionEvent.CommandEvent.Prepare;
import com.barracuda.engine.event.ExecutionEvent.CommandEvent.Reset;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowCompletedEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowFailedEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowPausedEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowStartedEvent;

import java.util.Objects;
import java.util.concurrent.StructuredTaskScope;

public class FlowImpl implements Flow {

    private final ChainNode nextNode;
    private volatile FlowStatus status = FlowStatus.READY;
    private final long flowID;
    private final FlowContext context;
    private volatile boolean startedEventPublished = false;

    public FlowImpl(ChainNode nextNode, FlowContext context, long flowID) {
        this.context = Objects.requireNonNull(context);
        this.nextNode = nextNode;
        this.flowID = flowID;
    }

    @Override
    public void event(ExecutionEvent event) {
        switch (event) {
            case FlowStartedEvent ev -> handleFlowStartedEvent(ev);
            case FlowCompletedEvent _ -> status = FlowStatus.COMPLETED; // already completed.
            case FlowFailedEvent ev -> {
                status = FlowStatus.FAILED;
                throw ev.exception();
            }
            case FlowPausedEvent _ -> status = FlowStatus.PAUSED;
            case CommandEvent command -> handleCommand(command);
            default -> propagateEvent(event);
        }
    }

    private void handleFlowStartedEvent(FlowStartedEvent startedEvent) {
        if (status != FlowStatus.REPLAY_MODE) {
            throw new IllegalStateException("Flow cannot accept events due to it being in the " + status + " state.");
        }

        if (startedEvent.flowID() != flowID) { // event isn't associated with this flow. Propagate the event further.
            propagateEvent(startedEvent);
            return;
        }

        if (startedEventPublished) {
            throw new IllegalStateException("Duplicate flow started event.");
        }

        startedEventPublished = true;
    }

    private void handleCommand(CommandEvent event) {
        switch (event) {
            case Continue ev -> handleContinueCommand(ev);
            case EnterReplayMode ev -> enterReplayMode(ev);
            case Reset ev -> reset(ev);
            case Prepare prepare -> prepare(prepare);
        }
    }

    private void prepare(Prepare prepare) {
        if (status != FlowStatus.REPLAY_MODE) {
            throw new IllegalStateException("Cannot prepare flow because of its current status " + status);
        }

        if (nextNode != null) {
            nextNode.event(prepare);
        }

        status = FlowStatus.READY;
    }

    private void reset(Reset reset) {
        if (status == FlowStatus.RUNNING) {
            throw new IllegalStateException("Cannot reset flow that is currently running.");
        }

        propagateEvent(reset);

        status = FlowStatus.READY;

        startedEventPublished = false;
    }

    private void enterReplayMode(EnterReplayMode enterReplayMode) {
        if (status != FlowStatus.READY) {
            throw new IllegalStateException("Flow cannot enter replay mode while in " + status + " state.");
        }

        if (nextNode != null) {
            nextNode.event(enterReplayMode);
        }

        status = FlowStatus.REPLAY_MODE;
    }

    private void handleContinueCommand(Continue continueEvent) {
        if (status != FlowStatus.READY) {
            throw new IllegalStateException("Cannot continue a flow that's in " + status + " state.");
        }

        status = FlowStatus.RUNNING;

        if (!startedEventPublished) {
            context.flowEventPublisher().publish(new FlowStartedEvent(flowID));
            startedEventPublished = true;
        }

        propagateEvent(continueEvent);

        context.flowEventPublisher().publish(new FlowCompletedEvent(flowID));

        status = FlowStatus.COMPLETED;
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
            case FlowInterruptedException ex -> handleInterruptedException(ex);
            case InterruptedException ex -> handleInterruptedException(new FlowInterruptedException("Flow Interrupted", ex));
            case StructuredTaskScope.FailedException ex -> handle(ex.getCause());
            case RuntimeException ex -> handleRuntimeException(ex);
            default -> handleRuntimeException(new RuntimeException(exception));
        }
    }

    private void handleInterruptedException(FlowInterruptedException ex) {
        Thread.currentThread().interrupt();
        assert status == FlowStatus.RUNNING;

        if (status != FlowStatus.RUNNING) {
            throw new IllegalStateException("Cannot interrupt a flow that's in " + status + " state.");
        }

        status = FlowStatus.PAUSED;
        context.flowEventPublisher().publish(new FlowPausedEvent(flowID));
        throw ex;
    }

    private void handleRuntimeException(RuntimeException ex) {
        if (status != FlowStatus.RUNNING && status != FlowStatus.REPLAY_MODE) {
            throw new IllegalStateException("Flow that is neither RUNNING nor in REPLAY_MODE has failed", ex);
        }
        status = FlowStatus.FAILED;
        context.flowEventPublisher().publish(new FlowFailedEvent(flowID, ex));

        throw ex;
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
