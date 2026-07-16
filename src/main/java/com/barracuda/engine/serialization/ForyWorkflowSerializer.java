package com.barracuda.engine.serialization;

import org.apache.fory.ThreadSafeFory;

public class ForyWorkflowSerializer implements WorkflowSerializer{


    private final ThreadSafeFory threadSafeFory;

    public ForyWorkflowSerializer(ThreadSafeFory threadSafeFory) {
        this.threadSafeFory = threadSafeFory;
    }


    @Override
    public byte[] serialize(Object object) {
        return threadSafeFory.serialize(object);
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> clazz) {
        return threadSafeFory.deserialize(bytes, clazz);
    }

}
