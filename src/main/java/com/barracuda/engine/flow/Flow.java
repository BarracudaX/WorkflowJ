package com.barracuda.engine.flow;

import com.barracuda.engine.step.Step;

public interface Flow {

    Step firstStep();

    Step nextStep();

}
