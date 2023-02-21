/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.backend.store.raft.rpc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.IteratorUtils;
import org.apache.hugegraph.backend.BackendException;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.backend.store.raft.RaftClosure;
import org.apache.hugegraph.backend.store.raft.RaftContext;
import org.apache.hugegraph.backend.store.raft.RaftStoreClosure;
import org.apache.hugegraph.backend.store.raft.StoreCommand;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.StoreAction;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.StoreCommandRequest;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.StoreCommandResponse;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.StoreType;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcRequestProcessor;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.google.protobuf.Message;
import com.google.protobuf.ZeroByteStringHelper;

public class StoreCommandProcessor
       extends RpcRequestProcessor<StoreCommandRequest> {

    private static final Logger LOG = Log.logger(StoreCommandProcessor.class);

    private final Map<Short, RaftContext> contexts;

    public StoreCommandProcessor(Map<Short, RaftContext> contexts) {
        super(null, null);
        this.contexts = contexts;
    }

    @Override
    public Message processRequest(StoreCommandRequest request,
                                  RpcRequestClosure done) {
        StoreCommandResponse.Builder response = StoreCommandResponse.newBuilder();
        try {
            short shardId = (short) request.getShardId();
            RaftContext context = this.contexts.get(shardId);
            if (StoreAction.QUERY.equals(request.getAction())) {
                byte[] result = query(context, request);
                response.setData(ZeroByteStringHelper.wrap(result));
            } else {
                StoreCommand command = this.parseStoreCommand(request);
                RaftStoreClosure closure = new RaftStoreClosure(command);
                context.node().submitAndWait(command, closure);
                // TODO: return the submitAndWait() result to rpc client
            }

            return response.setStatus(true).build();
        } catch (Throwable e) {
            LOG.warn("Failed to process StoreCommandRequest: {}",
                     request.getAction(), e);
            StoreCommandResponse.Builder builder = StoreCommandResponse
                                                   .newBuilder()
                                                   .setStatus(false);
            if (e.getMessage() != null) {
                builder.setMessage(e.getMessage());
            }
            return builder.build();
        }
    }

    @Override
    public String interest() {
        return StoreCommandRequest.class.getName();
    }

    private StoreCommand parseStoreCommand(StoreCommandRequest request) {
        StoreType type = request.getType();
        StoreAction action = request.getAction();
        byte[] data = request.getData().toByteArray();
        int shardId = request.getShardId();
        return new StoreCommand(type, action, data, true, (short) shardId);
    }

    private byte[] query(RaftContext raftContext, StoreCommandRequest request) throws Throwable {
        RaftClosure<Object> future = new RaftClosure<>();
        BackendStore backendStore = raftContext.originStore(request.getType());
        byte[] data = request.getData().toByteArray();
        Query query = (Query) new ObjectInputStream(new ByteArrayInputStream(data))
                      .readObject();

        ReadIndexClosure readIndexClosure = new ReadIndexClosure() {
            @Override
            public void run(Status status, long index, byte[] reqCtx) {
                if (status.isOk()) {
                    future.complete(status, () -> backendStore.query(query));
                } else {
                    future.failure(status, new BackendException(
                                           "Failed to do raft read-index: %s",
                                           status));
                }
            }
        };

        raftContext.node().readIndex(BytesUtil.EMPTY_BYTES, readIndexClosure);
        Object result = future.waitFinished();
        Iterator<BackendEntry> backendEntryIterator = (Iterator<BackendEntry>) result;

        List<BackendEntry> resultList = new ArrayList<>();
        resultList.addAll(IteratorUtils.toList(backendEntryIterator));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream sOut = new ObjectOutputStream(out);
        sOut.writeObject(resultList);
        sOut.flush();
        return out.toByteArray();
    }
}
