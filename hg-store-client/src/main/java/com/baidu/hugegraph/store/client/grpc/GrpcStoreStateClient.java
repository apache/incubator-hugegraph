package com.baidu.hugegraph.store.client.grpc;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

import com.baidu.hugegraph.pd.client.PDClient;
import com.baidu.hugegraph.pd.client.PDConfig;
import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.store.grpc.state.HgStoreStateGrpc;
import com.baidu.hugegraph.store.grpc.state.HgStoreStateGrpc.HgStoreStateBlockingStub;
import com.baidu.hugegraph.store.grpc.state.ScanState;
import com.baidu.hugegraph.store.grpc.state.SubStateReq;

import io.grpc.ManagedChannel;
import io.grpc.stub.AbstractBlockingStub;
import lombok.extern.slf4j.Slf4j;

/**
 *
 */
@Slf4j
@ThreadSafe
public class GrpcStoreStateClient extends AbstractGrpcClient {

    private final PDConfig pdConfig;
    private final PDClient pdClient;

    public GrpcStoreStateClient(PDConfig pdConfig) {
        this.pdConfig = pdConfig;
        pdClient = PDClient.create(this.pdConfig);
    }


    public Set<ScanState> getScanState() throws Exception {
        try {
            List<Metapb.Store> activeStores = pdClient.getActiveStores();
            Set<ScanState> states = activeStores.parallelStream().map(node -> {
                String address = node.getAddress();
                HgStoreStateBlockingStub stub = (HgStoreStateBlockingStub) getBlockingStub(address);
                SubStateReq req = SubStateReq.newBuilder().build();
                return stub.getScanState(req);
            }).collect(Collectors.toSet());
            return states;
        } catch (Exception e) {
            throw e;
        }


    }

    @Override
    public AbstractBlockingStub getBlockingStub(ManagedChannel channel) {
        HgStoreStateBlockingStub stub;
        stub = HgStoreStateGrpc.newBlockingStub(channel);
        return stub;
    }
}
