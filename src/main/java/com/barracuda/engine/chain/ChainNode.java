package com.barracuda.engine.chain;

import com.barracuda.engine.event.ExecutionEvent;

public interface ChainNode {

    void event(ExecutionEvent event);

}
