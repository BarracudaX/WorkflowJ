package com.barracuda.engine.flow;

/**
 * Used by flow code to indicate that the flow was interrupted.
 */
public class FlowInterruptedException extends RuntimeException {

    public FlowInterruptedException(String message) {
        super(message);
    }

    public FlowInterruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
