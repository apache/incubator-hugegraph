package com.baidu.hugegraph.election;

public interface StateMachineContext {

    Integer epoch();

    String node();

    RoleElectionStateMachine stateMachine();

    void epoch(Integer epoch);

    Config config();

    MetaDataAdapter adapter();

    void reset();
}
