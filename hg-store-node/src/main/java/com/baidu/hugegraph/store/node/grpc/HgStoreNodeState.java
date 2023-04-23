package com.baidu.hugegraph.store.node.grpc;

import com.baidu.hugegraph.store.grpc.state.NodeStateType;

import javax.annotation.concurrent.ThreadSafe;

/**
 * @author lynn.bond@hotmail.com created on 2021/11/3
 */
@ThreadSafe
public final class HgStoreNodeState {

    private static NodeStateType curState = NodeStateType.STARTING;

    public static NodeStateType getState() {
        return curState;
    }

    private static void setState(NodeStateType state) {
        curState = state;
        change();
    }

    private static void change() {
        HgStoreStateSubject.notifyAll(curState);
    }

    public static void goOnline() {
        setState(NodeStateType.ONLINE);
    }

    public static void goStarting() {
        setState(NodeStateType.STARTING);
    }

    public static void goStopping() {
        setState(NodeStateType.STOPPING);
    }

}
