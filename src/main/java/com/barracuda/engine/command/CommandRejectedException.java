package com.barracuda.engine.command;

public class CommandRejectedException extends RuntimeException {

    public CommandRejectedException(String message) {
        super(message);
    }

}
