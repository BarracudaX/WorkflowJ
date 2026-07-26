package com.barracuda.engine.flow;

import com.barracuda.engine.chain.ChainNode;

/**
 * Flow isn't thread safe. The thread safety of Flow instances must be achieved with thread confinement.
 * Implementations of this interface guarantee visibility but not atomicity. Hence, this class is not thread safe to be used by multiple threads for sending events.
 */
public interface Flow extends ChainNode {

    ScopedValue<FlowContext> FLOW_CONTEXT = ScopedValue.newInstance();

    FlowStatus status();

    long id();
}
