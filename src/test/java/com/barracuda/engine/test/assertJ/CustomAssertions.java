package com.barracuda.engine.test.assertJ;

import com.barracuda.engine.test.flow.TestFlow;
import org.assertj.core.api.Assertions;

public class CustomAssertions extends Assertions {

    public static TestFlowAssert assertThat(TestFlow testFlow) {
        return new TestFlowAssert(testFlow);
    }

}
