package com.barracuda.engine.test.task;

public sealed interface TestTaskInput<I> {

    record TestTaskDataInput<I>(I input) implements TestTaskInput<I> {
    }

    record TestTaskNullInput<I>() implements TestTaskInput<I>{

    }


}
