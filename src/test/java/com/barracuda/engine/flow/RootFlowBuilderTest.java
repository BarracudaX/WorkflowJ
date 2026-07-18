package com.barracuda.engine.flow;

import com.barracuda.engine.task.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.*;

@ExtendWith(OutputCaptureExtension.class)
public class RootFlowBuilderTest {

    private final RootFlowBuilder rootFlowBuilder = new RootFlowBuilder();

    @Test
    void shouldSpecifyTasksThatExecuteSequentially(CapturedOutput output) {
        Flow flow = rootFlowBuilder
                .step(new PrintTask("1"))
                .step(new PrintTask("2"))
                .step(new PrintTask("3"))
                .build();

        flow.execute();

        assertThat(output.getAll().lines().toList()).containsExactly("1", "2", "3");
    }

    @Test
    void shouldAllowCreationOfEmptyFlow() {
        assertThatCode(rootFlowBuilder::build).doesNotThrowAnyException();
    }


    private record PrintTask(String str) implements Task<Void, Void> {

        @Override
            public Void execute(Void input) {
                System.out.println(str);
                return null;
            }
        }

}
