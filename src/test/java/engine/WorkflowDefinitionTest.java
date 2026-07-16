package engine;

import com.barracuda.engine.domain.WorkflowStatus;
import com.barracuda.engine.store.FileSystemWorkflowStore;
import com.barracuda.engine.store.WorkflowStore;
import com.barracuda.engine.test.TestCpuTask;
import com.barracuda.engine.test.TestIOTask;
import com.barracuda.engine.work.ParallelWork;
import com.barracuda.engine.work.SequentialWork;
import com.barracuda.engine.workflow.RootWorkflow;
import com.barracuda.engine.workflow.RootWorkflowImpl;
import com.barracuda.engine.workflow.SubWorkflow;
import org.apache.fory.Fory;
import org.apache.fory.ThreadSafeFory;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WorkflowDefinitionTest {

    private final ExecutorService virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final ExecutorService cpuExecutorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private final ThreadSafeFory threadSafeFory = Fory
            .builder()
            .requireClassRegistration(true)
            .withXlang(false)
            .withRefTracking(true)
            .withCompatible(false)
            .buildThreadSafeFory();

    private final WorkflowStore workflowStore = new FileSystemWorkflowStore(threadSafeFory);

    @BeforeEach
    void setUp() {
        String pid = String.valueOf(ProcessHandle.current().pid());
        System.setProperty("MY_PID", pid);
    }

    @Test
    void test() throws InterruptedException, ExecutionException {

        RootWorkflow workflow = new RootWorkflowImpl(
                "Test Workflow",
                1,
                Duration.ofMillis(100),
                cpuExecutorService,
                workflowStore,
                List.of(
                        new ParallelWork(
                                "Parallel Work",
                                1,
                                List.of(
                                        new SubWorkflow("Subworkflow",1,List.of(
                                                new SequentialWork("TestSeqWork",1L,
                                                        List.of(
                                                                new TestIOTask(Duration.ofMillis(50),"TestIoTask",1, Duration.ofMillis(200)),
                                                                new TestCpuTask("TestCpuTask",2,Duration.ofMillis(150))
                                                        ))
                                        )),
                                        new SubWorkflow("Subworkflow",2,List.of(
                                                new SequentialWork("TestSeqWork",2L,
                                                        List.of(
                                                                new TestIOTask(Duration.ofMillis(50),"TestIoTask",3, Duration.ofMillis(200)),
                                                                new TestCpuTask("TestCpuTask",4,Duration.ofMillis(150))
                                                        ))
                                        )),
                                        new SubWorkflow("Subworkflow",3,List.of(
                                                new SequentialWork("TestSeqWork",3L,
                                                        List.of(
                                                                new TestIOTask(Duration.ofMillis(50),"TestIoTask",5, Duration.ofMillis(200)),
                                                                new TestCpuTask("TestCpuTask",6,Duration.ofMillis(150))
                                                        ))
                                        ))
                                ),
                                null
                        )
                )
        );

        var workflowExecution = virtualThreadExecutor.submit(workflow::execute);


        Thread.sleep(150);

        workflowExecution.cancel(true);

        Awaitility.await().until(() -> workflow.status() == WorkflowStatus.PAUSED);


        virtualThreadExecutor.submit(workflow::execute).get();

        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> workflow.status() == WorkflowStatus.COMPLETED);
    }

    private record Person(String firstName, String lastName){}

    @Test
    void name() {
        ThreadSafeFory threadSafeFory = Fory
                .builder()
                .requireClassRegistration(true)
                .withXlang(false)
                .withRefTracking(true)
                .withCompatible(false)
                .buildThreadSafeFory();

        threadSafeFory.register(Person.class);

        WorkflowStore store = new FileSystemWorkflowStore(threadSafeFory);


        var writePath = Path.of("/home/barracuda/tmp/workflow-1-1");
        var readPath = Path.of("/home/barracuda/tmp/workflow-1-1.completed");


//        System.out.println(store.store(new Person("John", "Doe"), writePath));
        System.out.println(store.load(Person.class,readPath));
    }


    //    @Test
//    void shouldBeAbleToDefineWorkflowDefinitions() {
//        WorkflowDefinition workflowDefinition = RootWorkflowDefinitionBuilder
//                .builder()
//                .workflowConfig(flowConfig -> {
//                    flowConfig.name("Flow Name");
//                }).work(step -> {
//                    step.name("Send email to employee");
//                }).decisionStep(decisionStep -> {
//                    decisionStep.name("Deciding Payment Increase");
//
//                    decisionStep
//                            .decision(flow -> {})
//                            .decision(flow -> {});
//
//                })
//                .parallelStep(parallelStep -> {
//                    parallelStep.name("Parallel Step").description("Parallel Step Description");
//
//                    parallelStep
//                            .path(flow -> {})
//                            .path(flow -> {})
//                            .path(flow -> {});
//                }).build();

//        WorkflowDefinitionVerifier
//                .verify(workflowDefinition, workflow -> {
//                    workFlow.hasName("Flow Name");
//                })
//                .step( stepVerifier -> {
//                    stepVerifier.hasName("Send email to employee");
//                })
//                .desicionStep( decisionStepVerifier -> {
//                    decisionStepVerifier.hasDecision( flow -> {
//
//                    });
//
//                    decisionStepVerifier.hasDecision( flow -> {
//
//                    });
//                })
//                .parallelStep( parallelStepVerifier -> {
//                    parallelStepVerifier.hasPath( flow -> {
//
//                    });
//                    parallelStepVerifier.hasPath(flow -> {
//
//                    });
//                    parallelStepVerifier.hasPath( flow -> {
//
//                    });
//                }).verify();


//    }
}
