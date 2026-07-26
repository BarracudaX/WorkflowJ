package com.barracuda.engine.utility;

import com.barracuda.engine.flow.FlowPrettyOutput;
import com.barracuda.engine.test.assertJ.TestFlowAssert;

import java.util.function.Supplier;

public class TestUtils {

    private TestUtils() {}

    /**
     * A decorator function for test readability that returns the provided value.
     * @param times
     * @throws IllegalArgumentException if times is negative
     */
    public static long times(long times){
        if(times < 0){
            throw new IllegalArgumentException("Times cannot be negative. Times value : "+times+".");
        }
        return times;
    }
}
