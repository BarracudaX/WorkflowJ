package com.barracuda.engine.store;

import java.nio.file.Path;

public interface WorkflowStore {

    /**
     * Stored data in the provided file. Renames the file and return the path to the renamed file.
     * @param data to be stored in the file.
     * @param file initial file in which data is stored.
     * @return path to the renamed file.
     * @param <T> type of the data.
     */
    <T> Path store(T data, Path file);

    <T> T load(Class<T> clazz, Path file);
}
