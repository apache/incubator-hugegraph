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

import org.apache.hugegraph.store.grpc.state.HgStoreStateGrpc;
import org.apache.hugegraph.store.grpc.state.NodeStateRes;
import org.apache.hugegraph.store.grpc.state.ScanState;
import org.apache.hugegraph.store.grpc.state.SubStateReq;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.protobuf.Empty;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * created on 2021/11/3
 */
@Slf4j
@GRpcService
public class HgStoreStateService extends HgStoreStateGrpc.HgStoreStateImplBase {

    @Autowired
    HgStoreStreamImpl impl;

    @Override
    public void subState(SubStateReq request, StreamObserver<NodeStateRes> observer) {
        HgStoreStateSubject.addObserver(request.getSubId(), observer);
    }

    @Override
    public void unsubState(SubStateReq request, StreamObserver<Empty> observer) {
        HgStoreStateSubject.removeObserver(request.getSubId());
    }

    @Override
    public void getScanState(SubStateReq request, StreamObserver<ScanState> observer) {
        ScanState state = impl.getState();
        observer.onNext(state);
        observer.onCompleted();
    }
}
