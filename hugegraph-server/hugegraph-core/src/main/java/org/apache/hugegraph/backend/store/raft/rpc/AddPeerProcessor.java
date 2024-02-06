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

package org.apache.hugegraph.backend.store.raft.rpc;

import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcRequestProcessor;
import com.google.protobuf.Message;
import org.apache.hugegraph.backend.store.raft.RaftContext;
import org.apache.hugegraph.backend.store.raft.RaftGroupManager;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.CommonResponse;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.AddPeerRequest;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.AddPeerResponse;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class AddPeerProcessor
       extends RpcRequestProcessor<AddPeerRequest> {

    private static final Logger LOG = Log.logger(AddPeerProcessor.class);

    private final RaftContext context;

    public AddPeerProcessor(RaftContext context) {
        super(null, null);
        this.context = context;
    }

    @Override
    public Message processRequest(AddPeerRequest request,
                                  RpcRequestClosure done) {
        LOG.debug("Processing AddPeerRequest {}", request.getClass());
        RaftGroupManager nodeManager = this.context.raftNodeManager();
        try {
            nodeManager.addPeer(request.getEndpoint());
            CommonResponse common = CommonResponse.newBuilder()
                                                  .setStatus(true)
                                                  .build();
            return AddPeerResponse.newBuilder().setCommon(common).build();
        } catch (Throwable e) {
            CommonResponse common = CommonResponse.newBuilder()
                                                  .setStatus(false)
                                                  .setMessage(e.toString())
                                                  .build();
            return AddPeerResponse.newBuilder().setCommon(common).build();
        }
    }

    @Override
    public String interest() {
        return AddPeerRequest.class.getName();
    }
}
