package com.barracuda.engine.chain;

import com.barracuda.engine.command.Command;
import com.barracuda.engine.event.ExecutionEvent;
import com.barracuda.engine.flow.FlowPrettyOutput;

public class EmptyNode implements ChainNode {

    @Override
    public void command(Command command) {

    }

    @Override
    public void event(ExecutionEvent event) {

    }

    @Override
    public void prettyPrint(FlowPrettyOutput output) {

    }

}
