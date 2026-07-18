package com.barracuda.engine.workflow;

import com.barracuda.engine.WorkflowUnitTest;
import com.barracuda.engine.step.AbstractStep;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

@WorkflowUnitTest
public class WorkflowTest {

    @Test
    void shouldBuildWorkflowWithSpecifiedName() {
        Workflow workflow = workflow().name("TEST_NAME").build();

        assertThat(workflow.name()).isEqualTo("TEST_NAME");
    }


    @Test
    void shouldThrowNPEWhenWorkflowNameNotSet() {
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

    private WorkflowBuilder workflow(){
        return Workflow.workflow().flow(flow -> flow.step(new TestStep()));
    }


    private static class TestStep extends AbstractStep {}


}
