package com.barracuda.engine.task;

/**
 * An abstraction of executable step. A step should be either IO or CPU bound.
 */
public sealed interface Task permits IOTask,CpuTask{

}
