package com.baidu.hugegraph.election;

public interface RoleElectionStateMachine {

    void shutdown();

    void apply(StateMachineCallback stateMachineCallback);
}
