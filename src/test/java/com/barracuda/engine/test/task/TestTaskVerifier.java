package com.barracuda.engine.test.task;

import com.barracuda.engine.utility.AwaitilityUtils;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

public class TestTaskVerifier<T> {

    private final TestTask<T> testTask;

    public TestTaskVerifier(TestTask<T> testTask) {
        this.testTask = testTask;
    }

    public TestTaskVerifier<T> received(T input) {
        assertThat(testTask.lastInput()).isEqualTo(input);
        return this;
    }

    public TestTaskVerifier<T> ranOnVirtualThread(){
        assertThat(testTask.lastTaskThread()).isEqualTo(TestTask.TaskThread.VIRTUAL);
        return this;
    }

    public TestTaskVerifier<T> ranOnPlatformThread(){
        assertThat(testTask.lastTaskThread()).isEqualTo(TestTask.TaskThread.PLATFORM);
        return this;
    }

    public TestTaskVerifier<T> hasNotStarted() {
        assertThat(testTask.state()).isEqualTo(TestTaskState.READY);
        return this;
    }

    public TestTaskVerifier<T> isRunning() {
        AwaitilityUtils.waitUntilTestTaskIsRunning(testTask, Duration.ofSeconds(1));
        return this;
    }

    public TestTaskVerifier<T> wasCancelled(){
        AwaitilityUtils.waitUntilTestTaskInterrupted(testTask, Duration.ofSeconds(1));
        return this;
    }
}
