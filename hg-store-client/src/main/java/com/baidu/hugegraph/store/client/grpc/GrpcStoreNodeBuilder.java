package com.baidu.hugegraph.store.client.grpc;

import com.baidu.hugegraph.store.client.HgPrivate;
import com.baidu.hugegraph.store.client.HgStoreNode;
import com.baidu.hugegraph.store.client.HgStoreNodeBuilder;
import com.baidu.hugegraph.store.client.HgStoreNodeManager;
import com.baidu.hugegraph.store.client.util.HgAssert;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author lynn.bond@hotmail.com created on 2021/10/12
 */
public class GrpcStoreNodeBuilder implements HgStoreNodeBuilder {

    private static GrpcStoreSessionClient sessionClient = new GrpcStoreSessionClient();
    private static GrpcStoreStreamClient streamClient = new GrpcStoreStreamClient();
    private static final AtomicLong ids = new AtomicLong(0);

    private Long nodeId;
    private String address;
    private HgStoreNodeManager nodeManager;

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
            this.nodeId = ids.addAndGet(-1l);
        }

        HgAssert.isFalse(this.nodeId == null, "nodeId can't to be null");
        HgAssert.isFalse(this.address == null, "address can't to be null");

        GrpcStoreNodeImpl node = new GrpcStoreNodeImpl(this.nodeManager, sessionClient, streamClient);
        node.setNodeId(this.nodeId);
        node.setAddress(this.address);

        return node;
    }
}
