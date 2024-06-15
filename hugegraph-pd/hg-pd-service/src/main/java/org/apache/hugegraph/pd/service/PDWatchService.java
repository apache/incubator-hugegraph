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

package org.apache.hugegraph.pd.service;

import org.apache.hugegraph.pd.grpc.watch.HgPdWatchGrpc;
import org.apache.hugegraph.pd.grpc.watch.WatchRequest;
import org.apache.hugegraph.pd.grpc.watch.WatchResponse;
import org.apache.hugegraph.pd.watch.PDWatchSubject;
import org.lognet.springboot.grpc.GRpcService;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@GRpcService
public class PDWatchService extends HgPdWatchGrpc.HgPdWatchImplBase {

    @Override
    public StreamObserver<WatchRequest> watch(StreamObserver<WatchResponse> responseObserver) {
        return PDWatchSubject.addObserver(responseObserver);
    }
}
