package com.barracuda.engine.flow;

import com.barracuda.engine.event.ExecutionEvent;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

import static org.mockito.Mockito.*;

public class SubflowDecoratorTest {

    private final static long rootID = 1;
    private final static long subflowID = 5;

    private final Flow subflowMock = mock(Flow.class);
    private final Flow flowDecorator = new SubflowDecorator(subflowMock);

    @BeforeEach
    void setUp() {
        when(subflowMock.id()).thenReturn(subflowID);
    }

    @Test
    void shouldTranslateSubflowStartedEventToFlowStartedEvent() {
        flowDecorator.event(new SubflowStartedEvent(rootID, subflowID));

        verify(subflowMock).event(new FlowStartedEvent(subflowID));
    }

    @Test
    void shouldNotTranslateSubflowStartedEventToFlowStartedEventWhenTheEventIsNotAssociatedWithTheSubflow() {
        var unrelatedSubflowStartedEvent = new SubflowStartedEvent(rootID, 555);
        flowDecorator.event(unrelatedSubflowStartedEvent);

        verify(subflowMock).event(unrelatedSubflowStartedEvent); // not translated
    }

    @Test
    void shouldTranslateSubflowCompletedEventToFlowCompletedEvent() {
        flowDecorator.event(new SubflowCompletedEvent(rootID,subflowID));

        verify(subflowMock).event(new FlowCompletedEvent(subflowID));
    }

    @Test
    void shouldNotTranslateSubflowCompletedEventToFlowCompletedEventWhenTheEventIsNotAssociatedWithTheSubflow() {
        var unrelatedSubflowCompletedEvent = new SubflowCompletedEvent(rootID, 555);
        flowDecorator.event(unrelatedSubflowCompletedEvent);

        verify(subflowMock).event(unrelatedSubflowCompletedEvent);
    }

    @Test
    void shouldTranslateSubflowPausedEventToFlowPausedEvent() {
        flowDecorator.event(new SubflowPausedEvent(rootID,subflowID));

        verify(subflowMock).event(new FlowPausedEvent(subflowID));
    }

    @Test
    void shouldNotTranslateSubflowPausedEventToFlowPausedEventWhenTheEventIsNotAssociatedWithTheSubflow() {
        var unrelatedSubflowPausedEvent = new SubflowPausedEvent(rootID, 555);
        flowDecorator.event(unrelatedSubflowPausedEvent);

        verify(subflowMock).event(unrelatedSubflowPausedEvent);
    }

    @Test
    void shouldTranslateSubflowFailedEventToFlowFailedEvent() {
        var exception = new RuntimeException("FAILED");
        flowDecorator.event(new SubflowFailedEvent(rootID, exception, subflowID));

        verify(subflowMock).event(new FlowFailedEvent(subflowID, exception));
    }

    @Test
    void shouldNotTranslateSubflowFailedEventToFlowFailedEventWhenTheEventIsNotAssociatedWithTheSubflow() {
        var exception = new RuntimeException("FAILED");
        var unrelatedSubflowFailedEvent = new SubflowFailedEvent(rootID, exception, 555);
        flowDecorator.event(unrelatedSubflowFailedEvent);

        verify(subflowMock).event(unrelatedSubflowFailedEvent);
    }

    @MethodSource("nonTranslatableEvents")
    @ParameterizedTest
    void shouldNotTranslateOtherEvents(ExecutionEvent otherEvent) {
        flowDecorator.event(otherEvent);

        verify(subflowMock).event(otherEvent);
    }

    private static List<ExecutionEvent> nonTranslatableEvents(){
        return List.of(
                new FlowStartedEvent(subflowID),new FlowCompletedEvent(subflowID),new FlowFailedEvent(subflowID,null),new FlowPausedEvent(subflowID),
                new TaskStartEvent(subflowID,110000),new TaskCompletedEvent(subflowID,10000), new TaskFailedEvent(subflowID,10000,null), new TaskPausedEvent(subflowID,10000)
        );
    }
}
