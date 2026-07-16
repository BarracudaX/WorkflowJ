package com.barracuda.engine.task;

import com.barracuda.engine.domain.TimeSlot;
import lombok.ToString;
import org.slf4j.MDC;

public non-sealed abstract class CpuTask implements Task {

    private final String name;
    private final long id;

    protected CpuTask(String name, long id) {
        this.name = name;
        this.id = id;
    }

    public final void execute(TimeSlot timeSlot) {

        MDC.put("task", " - ["+name+"-"+id+"]");
        MDC.put("taskType", " - [CPU]");
        try{
            executeTask(timeSlot);
        }finally {
            MDC.remove("task");
            MDC.remove("taskType");
        }

    }

    protected abstract void executeTask(TimeSlot timeSlot);
}
