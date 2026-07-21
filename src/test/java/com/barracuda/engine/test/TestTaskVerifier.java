package com.barracuda.engine.test;

import com.barracuda.engine.utility.AwaitilityUtils;

import static org.assertj.core.api.Assertions.assertThat;

public class TestTaskVerifier<T> {

    private final TestTask<T> testTask;

    TestTaskVerifier(TestTask<T> testTask) {
        this.testTask = testTask;
    }

    public TestTaskVerifier<T> received(T input) {
        assertThat(testTask.input()).isEqualTo(input);
        return this;
    }

    public TestTaskVerifier<T> ranOnVirtualThread(){
        assertThat(testTask.taskThread()).isEqualTo(TestTask.TaskThread.VIRTUAL);
        return this;
    }

    public TestTaskVerifier<T> ranOnPlatformThread(){
        assertThat(testTask.taskThread()).isEqualTo(TestTask.TaskThread.PLATFORM);
        return this;
    }

    public TestTaskVerifier<T> hasNotStarted() {
        assertThat(testTask.state()).isEqualTo(TestTaskState.CREATED);
        return this;
    }

    public TestTaskVerifier<T> isRunning() {
        AwaitilityUtils.waitUntilTestTaskIsRunning(testTask);
        return this;
    }

    public TestTaskVerifier<T> wasCancelled(){
        AwaitilityUtils.waitUntilTestTaskInterrupted(testTask);
        return this;
    }
}
