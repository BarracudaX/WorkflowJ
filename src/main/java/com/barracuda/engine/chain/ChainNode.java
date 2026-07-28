package com.barracuda.engine.chain;

import com.barracuda.engine.command.Command;
import com.barracuda.engine.event.ExecutionEvent;
import com.barracuda.engine.flow.FlowPrettyOutput;

public interface ChainNode {

    void command(Command command);

    void event(ExecutionEvent event);

    void prettyPrint(FlowPrettyOutput output);
}
