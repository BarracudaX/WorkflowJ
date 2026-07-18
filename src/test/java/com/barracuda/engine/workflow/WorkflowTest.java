package com.barracuda.engine.workflow;

import com.barracuda.engine.WorkflowUnitTest;
import com.barracuda.engine.step.AbstractStep;
import com.barracuda.engine.step.Step;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static com.barracuda.engine.workflow.Workflow.workflow;
import static org.assertj.core.api.Assertions.*;

@WorkflowUnitTest
public class WorkflowTest {

    @Test
    void shouldBuildWorkflowWithSpecifiedName() {
        Workflow workflow = workflow().name("TEST_NAME").build();

        assertThat(workflow.name()).isEqualTo("TEST_NAME");
    }

    @Test
    void shouldBuildWorkflowWithProvidedExecutionChain() {
        Step firstStep = new FirstStep();
        Step secondStep = new SecondStep();
        Step thirdStep = new ThirdStep();

        Workflow workflow = workflow()
                .name("TEST_NAME")
                .step(firstStep)
                .step(secondStep)
                .step(thirdStep)
                .build();

        assertThat(workflow.flow().firstStep()).isEqualTo(firstStep);
        assertThat(workflow.flow().nextStep()).isEqualTo(secondStep);
        assertThat(workflow.flow().nextStep().nextStep()).isEqualTo(thirdStep);
        assertThat(workflow.flow().nextStep().nextStep().nextStep()).isNull();
    }

    @Test
    void shouldThrowNullPointerExceptionWhenWorkflowNameNotSet() {
        assertThatThrownBy(() -> workflow().build()).isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldThrowIAEWhenProvidingNullName() {
        assertThatThrownBy(() -> workflow().name(null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldThrowIAEWhenProvidingBlankName() {
        assertThatThrownBy(() -> workflow().name(" ")).isInstanceOf(IllegalArgumentException.class);
    }

    private static class FirstStep extends AbstractStep { }
    private static class SecondStep extends AbstractStep { }
    private static class ThirdStep extends AbstractStep { }
}
