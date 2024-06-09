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

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hugegraph.store.grpc.state.NodeStateRes;
import org.apache.hugegraph.store.grpc.state.NodeStateType;
import org.apache.hugegraph.store.node.util.HgAssert;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * created on 2021/11/3
 */
@Slf4j
public final class HgStoreStateSubject {

    public final static Map<String, StreamObserver<NodeStateRes>> subObserverHolder =
            new ConcurrentHashMap<>();

    public static void addObserver(String subId, StreamObserver<NodeStateRes> observer) {
        HgAssert.isArgumentValid(subId, "subId");
        HgAssert.isArgumentNotNull(observer == null, "observer");

        subObserverHolder.put(subId, observer);
    }

    public static void removeObserver(String subId) {
        HgAssert.isArgumentValid(subId, "subId");
        subObserverHolder.remove(subId);
    }

    public static void notifyAll(NodeStateType nodeState) {

        HgAssert.isArgumentNotNull(nodeState == null, "nodeState");
        NodeStateRes res = NodeStateRes.newBuilder().setState(nodeState).build();
        Iterator<Map.Entry<String, StreamObserver<NodeStateRes>>> iter =
                subObserverHolder.entrySet().iterator();

        while (iter.hasNext()) {
            Map.Entry<String, StreamObserver<NodeStateRes>> entry = iter.next();

            try {
                entry.getValue().onNext(res);
            } catch (Throwable e) {
                log.error("Failed to send node-state[" + nodeState + "] to subscriber[" +
                          entry.getKey() + "].", e);
                iter.remove();
                log.error("Removed the subscriber[" + entry.getKey() + "].", e);
            }

        }
    }
}
