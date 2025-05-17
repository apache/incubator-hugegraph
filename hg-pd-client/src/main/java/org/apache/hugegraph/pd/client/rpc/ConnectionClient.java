package org.apache.hugegraph.pd.client.rpc;

import org.apache.hugegraph.pd.client.BaseClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.PDGrpc;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.common.NoArg;

/**
 * @author zhangyingjie
 * @date 2024/1/31
 **/
public class ConnectionClient extends BaseClient {
    public ConnectionClient(PDConfig pdConfig) {
        super(pdConfig, PDGrpc::newStub, PDGrpc::newBlockingStub);
    }

    public Pdpb.CacheResponse getClientCache() throws PDException {
        Pdpb.GetGraphRequest request = Pdpb.GetGraphRequest.newBuilder().setHeader(this.header).build();
        Pdpb.CacheResponse cache = blockingUnaryCall(PDGrpc.getGetCacheMethod(), request);
        handleErrors(cache.getHeader());
        return cache;
    }

    public Pdpb.CachePartitionResponse getPartitionCache(String graph) throws PDException {
        Pdpb.GetGraphRequest request =
                Pdpb.GetGraphRequest.newBuilder().setHeader(this.header).setGraphName(graph).build();
        Pdpb.CachePartitionResponse ps = blockingUnaryCall(PDGrpc.getGetPartitionsMethod(), request);
        handleErrors(ps.getHeader());
        return ps;
    }

    public Pdpb.GetAllGrpcAddressesResponse getPdAddressesCache() throws PDException {
        NoArg request = NoArg.newBuilder().build();
        Pdpb.GetAllGrpcAddressesResponse response =
                blockingUnaryCall(PDGrpc.getGetAllGrpcAddressesMethod(), request);
        handleErrors(response.getHeader());
        return response;
    }

    public void onLeaderChanged(String leader) {
    }
}
