package com.barracuda.engine.flow;

import com.barracuda.engine.step.AbstractStep;
import com.barracuda.engine.step.Step;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class RootFlowBuilderTest {

    @Test
    void shouldBuildFlowWithProvidedExecutionChain() {
        Step firstStep = new FirstStep();
        Step secondStep = new SecondStep();
        Step thirdStep = new ThirdStep();

        Flow flow = new RootFlowBuilder()
                .step(firstStep)
                .step(secondStep)
                .step(thirdStep)
                .build();

        assertThat(flow.step()).isEqualTo(firstStep);
        assertThat(flow.nextStep()).isEqualTo(secondStep);
        assertThat(flow.nextStep().nextStep()).isEqualTo(thirdStep);
        assertThat(flow.nextStep().nextStep().nextStep()).isNull();
    }

        @Test
    void shouldThrowNPEWhenBuildingFlowWithoutAnyStep() {
        assertThatThrownBy(() -> new RootFlowBuilder().build()).isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldThrowIAEWhenProvidingNullStep() {
        assertThatThrownBy(() -> new RootFlowBuilder().step(null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void settingOnlySingleStepShouldResultInCycle() {
        FirstStep step = new FirstStep();
        Flow flow = new RootFlowBuilder().step(step).build();

        assertThat(flow.step()).isEqualTo(step);
        assertThat(flow.nextStep()).isNull();
    }

    private static class FirstStep extends AbstractStep { }
    private static class SecondStep extends AbstractStep { }
    private static class ThirdStep extends AbstractStep { }
}
