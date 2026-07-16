package com.barracuda.engine.store;

import org.apache.fory.ThreadSafeFory;
import org.apache.fory.io.ForyInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Objects;
import java.util.Set;

public class FileSystemWorkflowStore implements WorkflowStore{

    private final ThreadSafeFory threadSafeFory;
    private static final FileAttribute<Set<PosixFilePermission>> permissions = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-r-----"));
    private static final Set<OpenOption> options = Set.of(StandardOpenOption.WRITE,StandardOpenOption.CREATE_NEW,StandardOpenOption.TRUNCATE_EXISTING);

    public FileSystemWorkflowStore(ThreadSafeFory threadSafeFory) {
        this.threadSafeFory = Objects.requireNonNull(threadSafeFory);
    }

    @Override
    public <T> Path store(T data, Path file) {

        var renameFile = file.resolveSibling(file.getFileName().toString()+".completed");

        try(var channel = FileChannel.open(file, options,permissions)){

            threadSafeFory.serialize(Channels.newOutputStream(channel), data);

            channel.force(true);

            Files.move(file, renameFile , StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

            return renameFile;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public <T> T load(Class<T> clazz, Path file) {

        try {
            return threadSafeFory.deserialize(new ForyInputStream(Channels.newInputStream(FileChannel.open(file,StandardOpenOption.READ))), clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

}
