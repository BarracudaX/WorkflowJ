package com.barracuda.engine.flow;

import com.barracuda.engine.task.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;

@ExtendWith(OutputCaptureExtension.class)
public class RootFlowBuilderTest {

    private final RootFlowBuilder rootFlowBuilder = new RootFlowBuilder();

    @Test
    void shouldSpecifyTasksThatExecuteSequentially(CapturedOutput output) {
        Flow flow = rootFlowBuilder
                .step(new PrintTask(),provide("1"), nothing())
                .step(new PrintTask(),provide("2"),nothing())
                .step(new PrintTask(),provide("3"),nothing())
                .build();

        flow.execute();

        assertThat(output.getAll().lines().toList()).containsExactly("1", "2", "3");
    }

    @Test
    void shouldAllowCreationOfEmptyFlow() {

        assertThatCode(rootFlowBuilder::build).doesNotThrowAnyException();

    }

    private static <T> Consumer<T> nothing(){
        return t -> {};
    }

    private static <T>Supplier<T> provide(T t){
        return () -> t;
    }

    private static class PrintTask implements Task<String, Void> {


        @Override
        public Void execute(String str) {
            System.out.println(str);
            return null;
        }
    }

}
