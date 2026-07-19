package com.barracuda.engine.flow;

import com.barracuda.engine.utility.AwaitilityUtils;
import com.barracuda.engine.builder.RootFlowBuilder;
import com.barracuda.engine.event.EvenPublisherImpl;
import com.barracuda.engine.event.FlowEventPublisher;
import com.barracuda.engine.event.InMemoryEventCapturer;
import com.barracuda.engine.test.ParallelTestTask;
import com.barracuda.engine.test.TestTaskVerifier;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.barracuda.engine.test.TestFlowBuilder.testFlow;
import static org.assertj.core.api.Assertions.*;


@ExtendWith(OutputCaptureExtension.class)
public class FlowTest {

    private final ExecutorService cpuTaskExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private final ExecutorService ioTaskExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final InMemoryEventCapturer eventCapturer = new InMemoryEventCapturer();
    private final FlowEventPublisher eventPublisher = new EvenPublisherImpl();
    private final RootFlowBuilder rootFlowBuilder = new RootFlowBuilder(cpuTaskExecutor, ioTaskExecutor, eventPublisher).withID(1L);

    @BeforeEach
    void setUp() {
        eventPublisher.subscribe(eventCapturer);
    }

    @Test
    void shouldExecuteTasksInSpecifiedOrder(CapturedOutput output) {
        Flow flow = rootFlowBuilder
                .runnableTask(() -> System.out.println("1"), 1L)
                .runnableTask(() -> System.out.println("2"), 2L)
                .runnableTask(() -> System.out.println("3"), 3L)
                .build();

        ioTaskExecutor.submit(flow::execute);
        AwaitilityUtils.waitUntilFlowCompleted(flow);

        assertThat(output.getAll().lines().toList()).containsExactly("1", "2", "3");
    }

    @Disabled("need to figure out how to assert sequentiality")
    @Test
    void shouldExecutedTasksSequentially() {
    }

    @Test
    void shouldAllowCreationOfEmptyFlow() {
        assertThatCode(rootFlowBuilder::build).doesNotThrowAnyException();
    }
//
    @Test
    void shouldExecuteIoAndCpuTasksOnDifferentExecutors() {

        //Note that testFlow by default runs IO tasks on virtual thread and cpu tasks on platform threads.
        testFlow()
                .task("IoTask")
                .cpuTask("CpuTask")
                .build()
                .startFlow()
                .finishTask("IoTask")
                .finishTask("CpuTask")
                .assertThatTask("IoTask",TestTaskVerifier::ranOnVirtualThread)
                .assertThatTask("CpuTask",TestTaskVerifier::ranOnPlatformThread);
    }

    @Test
    void newlyCreatedFlowShouldBeInCreatedState() {
        testFlow()
                .task("Task")
                .build()
                .expectFlowInCreatedState();
    }

    @Test
    void runningFlowShouldHaveRunningState() {
        testFlow()
                .task("Task")
                .build()
                .startFlow()
                .expectIsRunning();
    }

    @Test
    void shouldHaveCompletedStateOnceFinished() {
        testFlow()
                .task("Task")
                .build()
                .startFlow()
                .finishTask("Task")
                .expectFlowCompleted();
    }

    @Disabled("TODO")
    @Test
    void shouldThrowISEWhenTryingToExecuteAlreadyRunningFlow() {
    }

    @Test
    void shouldHaveFailedStateIfTaskFailsWithException() {
        var exception = new RuntimeException("FAILED");
        testFlow()
                .task("FailTask")
                .build()
                .startFlow()
                .failTask("FailTask", exception)
                .expectFlowFailed(exception);
    }

    @Test
    void shouldAllowExecutingTasksInParallelWithSubWorkflows() {
        var readinessLatch = new CountDownLatch(3);
        var barrierLatch = new CountDownLatch(1);

        var flow = rootFlowBuilder
                .parallel(parallel ->
                        parallel
                                .subflow(subflow -> subflow.ioTask(new ParallelTestTask(readinessLatch, barrierLatch, 1L)).withID(1L))
                                .subflow(subflow -> subflow.ioTask(new ParallelTestTask(readinessLatch, barrierLatch, 2L)).withID(2L))
                                .subflow(subflow -> subflow.ioTask(new ParallelTestTask(readinessLatch, barrierLatch, 3L)).withID(3L))
                ).build();

        ioTaskExecutor.submit(flow::execute);

        Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(readinessLatch::await);

        barrierLatch.countDown();

        AwaitilityUtils.waitUntilFlowCompleted(flow);
    }

    @Test
    void shouldHavePausedStateWhenInterrupted() {
        testFlow()
                .task("FirstTask")
                .build()
                .startFlow()
                .interruptFlow()
                .expectFlowPaused(); // this test method is unnecessary since interruptFlow implicitly verifies that the flow was paused
    }

    @Test
    void shouldCancelAllRunningTaskWhenPaused() {
        testFlow()
                .task("FirstTask")
                .build()
                .startFlow()
                .interruptFlow()
                .assertThatTask("FirstTask",TestTaskVerifier::wasCancelled);
    }

    @Test
    void shouldNotExecuteNextTaskWhenInterrupted() {

        testFlow()
                .task("FirstTask")
                .task("SecondTask")
                .build()
                .startFlow()
                .assertThatTask("FirstTask",TestTaskVerifier::isRunning)
                .interruptFlow()
                .assertThatTask("SecondTask",TestTaskVerifier::hasNotStarted);
    }

    @Test
    void shouldHaveFailedStateIfParallelSubflowFailsWithException() {
        var exception = new RuntimeException("FAILED");
        testFlow()
                .parallelFlows("ParallelTask1","ParallelTask2","ParallelTask3")
                .build()
                .startFlow()
                .assertThatTask("ParallelTask1",TestTaskVerifier::isRunning)
                .assertThatTask("ParallelTask2",TestTaskVerifier::isRunning)
                .assertThatTask("ParallelTask3",TestTaskVerifier::isRunning)
                .failTask("ParallelTask1",exception)
                .expectFlowFailed(exception);
    }

    @Test
    void shouldCancelParallelSubflowsIfOneOfThemFails() {
        testFlow()
                .parallelFlows("ParallelTask1","ParallelTask2","ParallelTask3")
                .build()
                .startFlow()
                .failTask("ParallelTask1",new RuntimeException("FAILED"))
                .assertThatTask("ParallelTask2",TestTaskVerifier::wasCancelled)
                .assertThatTask("ParallelTask3",TestTaskVerifier::wasCancelled);

    }

    @Test
    void shouldNotRunNextTaskWhenParallelSubflowFails() {
        testFlow()
                .parallelFlows("ParallelTask1")
                .task("NextTask")
                .build()
                .startFlow()
                .failTask("ParallelTask1",new RuntimeException("FAILED"))
                .assertThatTask("NextTask",TestTaskVerifier::hasNotStarted);

    }

    @Test
    void shouldExecuteTheNextTaskWhenParallelSubflowsComplete() {
        testFlow()
                .parallelFlows("ParallelTask1","ParallelTask2")
                .task("NextTask")
                .build()
                .startFlow()
                .finishTask("ParallelTask1")
                .assertThatTask("NextTask", TestTaskVerifier::hasNotStarted)
                .finishTask("ParallelTask2")
                .assertThatTask("NextTask", TestTaskVerifier::isRunning);
    }

    @Disabled("Need to figure out the best way to interrupt the task")
    @Test
    void shouldPauseFlowIfTaskInterrupted() {
    }

    @Disabled("Not yet sure how resumption will be implemented.")
    @Test
    void shouldAllowResumingPausedFlowByExecutingItAgain() {

    }

    @Test
    void shouldPublishFlowStartedEventWhenStartingTheFlow() {
        testFlow()
                .task("test")
                .build()
                .startFlow()
                .assertFlowEventsInOrder(events -> events.hasFlowStartedEvent().andHasNoMoreEvents());
    }

    @Test
    void shouldPublishTaskStartedEventWhenExecutingTheTask() {
        testFlow()
                .task("test")
                .build()
                .startFlow()
                .assertTaskEventsInOrder("test",events -> events.hasTaskStartedEvent().andHasNoMoreEvents());
    }

    @Test
    void shouldPublishTaskCompletedEventWhenTaskFinishesNormally() {
        testFlow()
                .task("test")
                .build()
                .startFlow()
                .finishTask("test")
                .assertTaskEventsInOrder("test",events -> events.hasTaskStartedEvent().hasTaskCompletedEvent().andHasNoMoreEvents());
    }

    @Test
    void shouldPublishFlowCompletedEventWhenFlowFinishesNormally() {
        testFlow()
                .task("test")
                .build()
                .startFlow()
                .finishTask("test")
                .expectFlowCompleted()
                .assertFlowEventsInOrder(events -> events.hasFlowStartedEvent().hasFlowCompletedEvent().andHasNoMoreEvents());
    }

    @Test
    void shouldPublishFlowFailedEventWhenATaskFailsWithAnException() {
        var exception = new RuntimeException("FAILED");

        testFlow()
                .task("test")
                .build()
                .startFlow()
                .failTask("test", exception)
                .expectFlowFailed()
                .assertFlowEventsInOrder( events -> events.hasFlowStartedEvent().hasFlowFailedEvent(exception).andHasNoMoreEvents());

    }

    @Test
    void shouldPublishTaskFailedEventWhenTaskFinishesWithAnException() {
        var exception = new RuntimeException("FAILED");

        testFlow()
                .task("test")
                .build()
                .startFlow()
                .failTask("test", exception)
                .expectFlowFailed()
                .assertTaskEventsInOrder("test",events -> events.hasTaskStartedEvent().hasTaskFailedEvent(exception).andHasNoMoreEvents());
    }

    @Test
    void shouldPublishFlowPausedEventWhenInterrupted() {
        testFlow()
                .task("test")
                .build()
                .startFlow()
                .interruptFlow()
                .assertFlowEventsInOrder(events -> events.hasFlowStartedEvent().hasFlowPausedEvent().andHasNoMoreEvents());
    }

    @Test
    void shouldPublishTaskPausedEventWhenTaskInterrupted() {
        testFlow()
                .task("test")
                .build()
                .startFlow()
                .interruptFlow()
                .assertTaskEventsInOrder("test", events -> events.hasTaskStartedEvent().hasTaskPausedEvent().andHasNoMoreEvents());
    }

}
