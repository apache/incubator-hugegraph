/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.store.node.grpc;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.hugegraph.store.grpc.state.NodeStateType;

/**
 * created on 2021/11/3
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
