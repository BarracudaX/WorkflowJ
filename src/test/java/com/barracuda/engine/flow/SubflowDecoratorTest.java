package com.barracuda.engine.flow;

import com.barracuda.engine.event.ExecutionEvent;
import com.barracuda.engine.event.ExecutionEvent.CommandEvent.Continue;
import com.barracuda.engine.event.ExecutionEvent.CommandEvent.EnterReplayMode;
import com.barracuda.engine.event.ExecutionEvent.CommandEvent.Prepare;
import com.barracuda.engine.event.ExecutionEvent.CommandEvent.Reset;
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

public class SubflowDecoratorTest {

    private final Flow subflowMock = mock(Flow.class);
    private final Flow flowDecorator = new SubflowDecorator(subflowMock);

    @Test
    void shouldTranslateSubflowStartedEventToFlowStartedEvent() {
        flowDecorator.event(new SubflowStartedEvent(1, 5));

        verify(subflowMock).event(new FlowStartedEvent(5));
    }

    @Test
    void shouldTranslateSubflowCompletedEventToFlowCompletedEvent() {
        flowDecorator.event(new SubflowCompletedEvent(1, 5));

        verify(subflowMock).event(new FlowCompletedEvent(5));
    }

    @Test
    void shouldTranslateSubflowPausedEventToFlowPausedEvent() {
        flowDecorator.event(new SubflowPausedEvent(1, 5));

        verify(subflowMock).event(new FlowPausedEvent(5));
    }

    @Test
    void shouldTranslateSubflowFailedEventToFlowFailedEvent() {
        var exception = new RuntimeException("FAILED");
        flowDecorator.event(new SubflowFailedEvent(1, 5, exception));

        verify(subflowMock).event(new FlowFailedEvent(5, exception));
    }

    @MethodSource("nonTranslatableEvents")
    @ParameterizedTest
    void shouldNotTranslateOtherEvents(ExecutionEvent event) {
        flowDecorator.event(event);

        verify(subflowMock).event(event);
    }

    private static List<ExecutionEvent> nonTranslatableEvents(){
        return List.of(
                new Continue(),new Reset(),new EnterReplayMode(),new Prepare(), new FlowStartedEvent(5),new FlowCompletedEvent(5),new FlowFailedEvent(5,null),new FlowPausedEvent(5),
                new TaskStartEvent(5,1),new TaskCompletedEvent(5,1), new TaskFailedEvent(5,1,null), new TaskPausedEvent(5,1)
        );
    }
}
