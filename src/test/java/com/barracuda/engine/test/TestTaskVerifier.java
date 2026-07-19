package com.barracuda.engine.test;

import com.barracuda.engine.utility.AwaitilityUtils;

import static org.assertj.core.api.Assertions.assertThat;

public class TestTaskVerifier {

    private final TestTask testTask;

    TestTaskVerifier(TestTask testTask) {
        this.testTask = testTask;
    }

    public TestTaskVerifier ranOnVirtualThread(){
        assertThat(testTask.taskThread()).isEqualTo(TestTask.TaskThread.VIRTUAL);
        return this;
    }

    public TestTaskVerifier ranOnPlatformThread(){
        assertThat(testTask.taskThread()).isEqualTo(TestTask.TaskThread.PLATFORM);
        return this;
    }

    public TestTaskVerifier hasNotStarted() {
        assertThat(testTask.state()).isEqualTo(TestTaskState.CREATED);
        return this;
    }

    public TestTaskVerifier isRunning() {
        AwaitilityUtils.waitUntilTestTaskIsRunning(testTask);
        return this;
    }

    public TestTaskVerifier wasCancelled(){
        AwaitilityUtils.waitUntilTestTaskInterrupted(testTask);
        return this;
    }
}
