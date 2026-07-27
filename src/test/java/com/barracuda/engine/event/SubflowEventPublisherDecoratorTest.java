package com.barracuda.engine.event;

import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowCompletedEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowFailedEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowPausedEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent.FlowStartedEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.SubflowCompletedEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.SubflowFailedEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.SubflowPausedEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent.SubflowStartedEvent;
import com.barracuda.engine.event.ExecutionEvent.TaskEvent.TaskCompletedEvent;
import com.barracuda.engine.event.ExecutionEvent.TaskEvent.TaskFailedEvent;
import com.barracuda.engine.event.ExecutionEvent.TaskEvent.TaskPausedEvent;
import com.barracuda.engine.event.ExecutionEvent.TaskEvent.TaskStartEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class SubflowEventPublisherDecoratorTest {

    private final static long rootFlowID = 1;
    private final static long subflowID = 5;

    private final FlowEventPublisher eventPublisherMock = mock(FlowEventPublisher.class);
    private final FlowEventPublisher decorator = new SubflowEventPublisherDecorator(subflowID,rootFlowID,eventPublisherMock);

    @Test
    void shouldTranslateFlowStartedEventToSubflowStartedEvent() {
        decorator.publish(new FlowStartedEvent(subflowID));

        verify(eventPublisherMock).publish(new SubflowStartedEvent(rootFlowID,subflowID));
    }

    @Test
    void shouldTranslateFlowCompletedEventToSubflowCompletedEvent() {
        decorator.publish(new FlowCompletedEvent(subflowID));

        verify(eventPublisherMock).publish(new SubflowCompletedEvent(rootFlowID,subflowID));
    }

    @Test
    void shouldTranslateFlowFailedEventToSubflowFailedEvent() {
        var exception = new RuntimeException("ERROR");

        decorator.publish(new FlowFailedEvent(subflowID, exception));

        verify(eventPublisherMock).publish(new SubflowFailedEvent(rootFlowID, exception, subflowID));
    }

    @Test
    void shouldTranslateFlowPausedEventToSubflowPausedEvent() {
        decorator.publish(new FlowPausedEvent(subflowID));

        verify(eventPublisherMock).publish(new SubflowPausedEvent(rootFlowID,subflowID));
    }

    @Test
    void shouldNotTranslateFlowStartedEventToSubflowStartedEventWhenTheEventIsNotAssociatedWithTheSubflow() {
        var unrelatedFlowStartedEvent = new FlowStartedEvent(777);

        decorator.publish(unrelatedFlowStartedEvent);

        verify(eventPublisherMock).publish(unrelatedFlowStartedEvent);
    }

    @Test
    void shouldNotTranslateFlowCompletedEventToSubflowCompletedEventWhenTheEventIsNotAssociatedWithTheSubflow() {
        var unrelatedFlowCompletedEvent = new FlowCompletedEvent(777);

        decorator.publish(unrelatedFlowCompletedEvent);

        verify(eventPublisherMock).publish(unrelatedFlowCompletedEvent);
    }

    @Test
    void shouldNotTranslateFlowFailedEventToSubflowFailedEventWhenTheEventIsNotAssociatedWithTheSubflow() {
        var exception = new RuntimeException("ERROR");
        var unrelatedFlowFailedEvent = new FlowFailedEvent(777, exception);

        decorator.publish(unrelatedFlowFailedEvent);

        verify(eventPublisherMock).publish(unrelatedFlowFailedEvent);
    }

    @Test
    void shouldNotTranslateFlowPausedEventToSubflowPausedEventWhenTheEventIsNotAssociatedWithTheSubflow() {
        var unrelatedFlowPausedEvent = new FlowPausedEvent(777);

        decorator.publish(unrelatedFlowPausedEvent);

        verify(eventPublisherMock).publish(unrelatedFlowPausedEvent);
    }

    @MethodSource("otherEvents")
    @ParameterizedTest
    void shouldNotTranslateOtherEvents(ExecutionEvent otherEvent) {
        decorator.publish(otherEvent);

        verify(eventPublisherMock).publish(otherEvent);
    }

    private static List<ExecutionEvent> otherEvents(){
        return List.of(
                new TaskStartEvent(subflowID, 110000), new TaskCompletedEvent(subflowID, 10000),
                new TaskFailedEvent(subflowID, 10000, null), new TaskPausedEvent(subflowID, 10000), new SubflowStartedEvent(rootFlowID, subflowID),
                new SubflowCompletedEvent(rootFlowID,subflowID), new SubflowFailedEvent(rootFlowID,null,subflowID), new SubflowPausedEvent(rootFlowID,subflowID)
        );
    }
}
