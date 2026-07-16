package com.barracuda.engine.serialization;

public interface WorkflowSerializer {


    byte[] serialize(Object object);

    <T> T deserialize(byte[] bytes,Class<T> clazz);

}
