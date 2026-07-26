package com.barracuda.engine.test.assertJ;

import com.barracuda.engine.event.ExecutionEvent;
import com.barracuda.engine.event.ExecutionEvent.FlowEvent;
import com.barracuda.engine.event.ExecutionEvent.SubflowEvent;
import com.barracuda.engine.event.ExecutionEvent.TaskEvent;
import com.barracuda.engine.test.flow.TestFlow;
import com.barracuda.engine.test.task.TestTask;
import com.barracuda.engine.test.task.TestTaskState;
import com.barracuda.engine.utility.AwaitilityUtils;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;

import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.SequencedCollection;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class TestFlowAssert extends AbstractAssert<TestFlowAssert, TestFlow> {

    protected TestFlowAssert(TestFlow testFlow) {
        super(testFlow, TestFlowAssert.class);
    }

    public TestFlowAssert isEventuallyCompleted() {
        return run(() -> "Failed waiting for flow to complete.",() -> AwaitilityUtils.waitUntilFlowCompleted(actual().getFlow(),Duration.ofSeconds(1)));
    }

    public TestFlowAssert isEventuallyRunning(){
        return run(() -> "Failed waiting for flow to begin running.", () -> AwaitilityUtils.waitUntilFlowRunning(actual.getFlow(), Duration.ofSeconds(1)));
    }

    public TestFlowAssert hasEventuallyFailedWith(RuntimeException exception) {

        run(() -> "Failed waiting for flow to fail with exception "+exception, () -> AwaitilityUtils.waitUntilFlowFailed(actual.getFlow(),Duration.ofSeconds(1)));

        return run(() -> "Expected the flow to have failed with " + exception, () -> Assertions.assertThatThrownBy(() -> actual.getFlowTask().get()).hasCause(exception));
    }

    public TestFlowAssert isEventuallyPaused(){
        return run(() -> "Expected the flow to pause.", () -> AwaitilityUtils.waitUntilFlowPaused(actual.getFlow(),Duration.ofSeconds(1)));
    }

    public TestFlowAssert enteredEventuallyReplayMode(){
        return run(() -> "Expected the flow to enter replay mode.", () -> AwaitilityUtils.waitUntilFlowInReplayMode(actual.getFlow(),Duration.ofSeconds(1)));
    }

    public TestFlowAssert hasTaskSatisfying(String taskName, Consumer<TestTaskAssert> taskVerifier) {
        taskVerifier.accept(new TestTaskAssert(actual.getTestTaskByName(taskName)));
        return this;
    }

    public TestFlowAssert flowEventsSatisfying(Consumer<ExecutionEventsAssert<FlowEvent>> verifier){
        verifier.accept(new ExecutionEventsAssert<>(actual.flowEvents()));
        return this;
    }

    public TestFlowAssert subflowEventsSatisfying(String subflow, Consumer<ExecutionEventsAssert<SubflowEvent>> verifier){
        verifier.accept(new ExecutionEventsAssert<>(actual.subflowEvents(subflow)));
        return this;
    }

    public TestFlowAssert taskEventsSatisfying(String taskName, Consumer<ExecutionEventsAssert<TaskEvent>> verifier) {
        verifier.accept(new ExecutionEventsAssert<>(actual.taskEvents(taskName)));
        return this;
    }

    public class TestTaskAssert extends AbstractAssert<TestTaskAssert, TestTask<?>> {

        TestTaskAssert(TestTask testTask) {
            super(testTask, TestTaskAssert.class);
        }

        public TestTaskAssert ranOnVirtualThread(){
            run(() -> "Expected the task " + actual.name() + " to have ran on virtual thread. Task: " + actual, () -> Assertions.assertThat(actual.lastTaskThread()).isEqualTo(TestTask.TaskThread.VIRTUAL));
            return this;
        }

        public TestTaskAssert ranOnPlatformThread(){
            run(() -> "Expected the task " + actual.name() + " to have ran on platform thread. Task: " + actual, () -> Assertions.assertThat(actual.lastTaskThread()).isEqualTo(TestTask.TaskThread.PLATFORM));
            return this;
        }

        public TestTaskAssert isEventuallyCancelled() {
            run(() -> "Expected the task "+actual.name()+" to eventually be canceled. Task: "+actual,() -> AwaitilityUtils.waitUntilTestTaskInterrupted(actual,Duration.ofSeconds(1)));
            return this;
        }

        public TestTaskAssert hasNotStarted(){
            run(() -> "Expected the task " + actual.name() + " to not be running. Task: " + actual, () -> Assertions.assertThat(actual.state()).isEqualTo(TestTaskState.READY));
            return this;
        }

        public TestTaskAssert isRunning(){
            run(() -> "Expected the task "+ actual.name()+" to be running. Task: "+actual,() -> AwaitilityUtils.waitUntilTestTaskIsRunning(actual,Duration.ofSeconds(1)));
            return this;
        }

    }

    public class ExecutionEventsAssert<E extends ExecutionEvent> extends AbstractAssert<ExecutionEventsAssert<E>, SequencedCollection<E>>{

        protected ExecutionEventsAssert(SequencedCollection<E> flowEvents) {
            super(flowEvents, ExecutionEventsAssert.class);
        }

//        public FlowEventsAssert nextEventIsFlowCompletedEvent(){
//            nextEventIs(FlowCompletedEvent.class);
//
//            return this;
//        }
//
//        public FlowEventsAssert nextEventIsFlowStartedEvent(){
//            nextEventIs(FlowStartedEvent.class);
//
//            return this;
//        }
//
//        public FlowEventsAssert nextEventIsFlowPausedEvent(){
////            nextEventIs(FlowEvent.FlowPausedEvent.class);
//
//            return this;
//        }
//
//        public FlowEventsAssert nextEventIsFlowFailedEvent(RuntimeException exception){
////            var event = nextEventIs(FlowEvent.FlowFailedEvent.class);
////
////            try{
////                Assertions.assertThat(event.exception()).isInstanceOf(exception.getClass()).hasMessage(exception.getMessage());
////            }catch (AssertionError error){
////                failWithThisMessage("Expected FlowFailedEvent's exception to be"+exception+" but was "+event.exception());
////            }
//
//            return this;
//        }

        public <T extends ExecutionEvent> ExecutionEventsAssert<E> nextEventIs(Class<T> clazz){
            return nextEventIs(clazz,(_) -> {});
        }

        public <T extends ExecutionEvent> ExecutionEventsAssert<E> nextEventIs(Class<T> clazz, Consumer<T> verifier){
            T event = null;
            try {
                event = (T) actual.removeFirst();
                clazz.cast(event);
            } catch (NoSuchElementException e) {
                throwWithMessage("No more events left. All events of this assertions have been exhausted/verified.", e);
            } catch (ClassCastException ex) {
                throwWithMessage("Expected next event to be " + clazz.getSimpleName() +", but was "+event, ex);
            } catch (Throwable ex) {
                throwWithMessage("Expected "+ clazz.getSimpleName() +" event.", ex);
            }

            verifier.accept(event);

            return this;
        }

        public ExecutionEventsAssert<E> andHasNoMoreEvents(){
            if (!actual.isEmpty()) {
                failWithThisMessage("Expected not to have events left, but remaining events were:"+actual);
            }
            return this;
        }
    }

    public class SubflowEventsAssert extends AbstractAssert<SubflowEventsAssert, SequencedCollection<SubflowEvent>> {

        protected SubflowEventsAssert(SequencedCollection<SubflowEvent> subflowEvents) {
            super(subflowEvents, SubflowEventsAssert.class);
        }

        public SubflowEventsAssert nextEventIsSubflowStartedEvent(){

            return this;
        }

        private <T extends SubflowEvent> T nextEventIs(Class<T> clazz){
            T event = null;
            try {
                event = (T) actual.removeFirst();
                clazz.cast(event);
            } catch (NoSuchElementException e) {
                throwWithMessage("No more events left. All events of this assertions have been exhausted/verified.", e);
            } catch (ClassCastException ex) {
                throwWithMessage("Expected next event to be " + clazz.getCanonicalName() +", but was "+event, ex);
            } catch (Throwable ex) {
                throwWithMessage("Expected "+ clazz.getCanonicalName() +" event.", ex);
            }
            return event;
        }

        public SubflowEventsAssert andHasNoMoreEvents(){
            if (!actual.isEmpty()) {
                failWithThisMessage("Expected not to have any subflow events left. Remaining events:"+actual);
            }
            Assertions.assertThat(actual).describedAs(() -> "Expected not to have events left. Remaining events: "+actual).isEmpty();
            return this;
        }
    }

    private TestFlowAssert run(Supplier<String> failMessage, Runnable runnable) {
        try{
            runnable.run();
        }catch (Throwable ex){
            throwWithMessage(failMessage.get(), ex);
        }

        return this;
    }

    private void throwWithMessage(String failMessage, Throwable ex) {
        System.err.println(actual.context());
        var error = failure(failMessage);
        error.initCause(ex);
        throw error;
    }

    private void failWithThisMessage(String failMessage) {
        System.err.println(actual.context());
        failWithMessage(failMessage);
    }

}
