package com.barracuda.engine.flow;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static com.barracuda.engine.test.builder.TestFlowBuilder.testFlow;


public class DataLoadingTest {

    @Test
    void shouldLoadDataIntoTask() {
        var data = new String[]{ "1", "2", "3"};
        testFlow()
                .consumerTask("task",String[].class,() -> data)
                .build()
                .startFlow()
                .assertConsumerTaskInput("task", String[].class, task -> task.received(data));
    }

    @Disabled("TODO")
    @Test
    void shouldLoadDataIntoTaskWithChunks() {

    }
}
