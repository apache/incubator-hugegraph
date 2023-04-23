package com.baidu.hugegraph.store.client.grpc;

import java.util.Objects;

import com.baidu.hugegraph.store.HgStoreSession;
import com.baidu.hugegraph.store.client.HgStoreNode;
import com.baidu.hugegraph.store.client.HgStoreNodeManager;

/**
 * @author lynn.bond@hotmail.com created on 2021/10/11
 */
class GrpcStoreNodeImpl implements HgStoreNode {
    private GrpcStoreSessionClient sessionClient;
    private GrpcStoreStreamClient streamClient;
    private HgStoreNodeManager nodeManager;
    private String address;
    private Long nodeId;

    GrpcStoreNodeImpl(HgStoreNodeManager nodeManager, GrpcStoreSessionClient sessionClient,
                      GrpcStoreStreamClient streamClient) {
        this.nodeManager = nodeManager;
        this.sessionClient = sessionClient;
        this.streamClient = streamClient;
    }

    GrpcStoreNodeImpl setNodeId(Long nodeId) {
        this.nodeId = nodeId;
        return this;
    }

    GrpcStoreNodeImpl setAddress(String address) {
        this.address = address;
        return this;
    }

    @Override
    public Long getNodeId() {
        return this.nodeId;
    }

    @Override
    public String getAddress() {
        return this.address;
    }

    @Override
    public HgStoreSession openSession(String graphName) {
        // HgAssert.isFalse(HgAssert.isInvalid(graphName), "the argument: graphName is invalid.");
        // return new GrpcStoreNodeSessionImpl2(this, graphName,this.nodeManager, this.sessionClient, this
        // .streamClient);
        return new GrpcStoreNodeSessionImpl(this, graphName, this.nodeManager, this.sessionClient, this.streamClient);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GrpcStoreNodeImpl that = (GrpcStoreNodeImpl) o;
        return Objects.equals(address, that.address) && Objects.equals(nodeId, that.nodeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(address, nodeId);
    }

    @Override
    public String toString() {
        return "storeNode: {" +
               "address: \"" + address + "\"" +
               ", nodeId: " + nodeId +
               "}";
    }
}
