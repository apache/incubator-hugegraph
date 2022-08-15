package com.baidu.hugegraph.election;

public interface StateMachineCallback {

    void master(StateMachineContext context);

    void worker(StateMachineContext context);

    void candidate(StateMachineContext context);

    void unknown(StateMachineContext context);

    void safe(StateMachineContext context);

    void error(StateMachineContext context, Throwable e);
}
