package com.barracuda.engine.task;

import lombok.ToString;
import org.slf4j.MDC;

@ToString
public non-sealed abstract class IOTask implements Task{

    private final String name;
    private final long id;

    protected IOTask(String name, long id) {
        this.name = name;
        this.id = id;
    }


    public final void execute(){

        MDC.put("task", " - ["+name+"-"+id+"]");
        MDC.put("taskType", " - [IO]");
        try{
            executeTask();
        }finally {
            MDC.remove("task");
            MDC.remove("taskType");
        }
    }

    protected abstract void executeTask();
}
