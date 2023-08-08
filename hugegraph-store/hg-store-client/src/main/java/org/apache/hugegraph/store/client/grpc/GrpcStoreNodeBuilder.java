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

package org.apache.hugegraph.store.client.grpc;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hugegraph.store.client.HgPrivate;
import org.apache.hugegraph.store.client.HgStoreNode;
import org.apache.hugegraph.store.client.HgStoreNodeBuilder;
import org.apache.hugegraph.store.client.HgStoreNodeManager;
import org.apache.hugegraph.store.client.util.HgAssert;

/**
 * created on 2021/10/12
 */
public class GrpcStoreNodeBuilder implements HgStoreNodeBuilder {

    private static final GrpcStoreSessionClient sessionClient = new GrpcStoreSessionClient();
    private static final GrpcStoreStreamClient streamClient = new GrpcStoreStreamClient();
    private static final AtomicLong ids = new AtomicLong(0);
    private final HgStoreNodeManager nodeManager;
    private Long nodeId;
    private String address;

    public GrpcStoreNodeBuilder(HgStoreNodeManager nodeManager, HgPrivate hgPrivate) {
        HgAssert.isArgumentNotNull(hgPrivate, "hgPrivate");
        HgAssert.isArgumentNotNull(nodeManager, "nodeManager");
        this.nodeManager = nodeManager;
    }

    @Override
    public GrpcStoreNodeBuilder setAddress(String address) {
        HgAssert.isFalse(HgAssert.isInvalid(address), "The argument is invalid: address.");
        this.address = address;
        return this;
    }

    @Override
    public GrpcStoreNodeBuilder setNodeId(Long nodeId) {
        HgAssert.isFalse(nodeId == null, "The argument is invalid: nodeId.");
        this.nodeId = nodeId;
        return this;
    }

    @Override
    public HgStoreNode build() {
        // TODO: delete
        if (this.nodeId == null) {
            this.nodeId = ids.addAndGet(-1L);
        }

        HgAssert.isFalse(this.nodeId == null, "nodeId can't to be null");
        HgAssert.isFalse(this.address == null, "address can't to be null");

        GrpcStoreNodeImpl node =
                new GrpcStoreNodeImpl(this.nodeManager, sessionClient, streamClient);
        node.setNodeId(this.nodeId);
        node.setAddress(this.address);

        return node;
    }
}
