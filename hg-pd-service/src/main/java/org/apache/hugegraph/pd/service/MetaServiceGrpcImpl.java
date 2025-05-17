package org.apache.hugegraph.pd.service;

import java.util.List;

import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.GraphSpaces;
import org.apache.hugegraph.pd.grpc.Graphs;
import org.apache.hugegraph.pd.grpc.MetaServiceGrpc;
import org.apache.hugegraph.pd.grpc.MetaServiceGrpc.MetaServiceImplBase;
import org.apache.hugegraph.pd.grpc.Metapb.Graph;
import org.apache.hugegraph.pd.grpc.Metapb.GraphSpace;
import org.apache.hugegraph.pd.grpc.Metapb.Partition;
import org.apache.hugegraph.pd.grpc.Metapb.ShardGroup;
import org.apache.hugegraph.pd.grpc.Metapb.Store;
import org.apache.hugegraph.pd.grpc.Partitions;
import org.apache.hugegraph.pd.grpc.ShardGroups;
import org.apache.hugegraph.pd.grpc.Stores;
import org.apache.hugegraph.pd.grpc.common.NoArg;
import org.apache.hugegraph.pd.grpc.common.ResponseHeader;
import org.apache.hugegraph.pd.grpc.common.VoidResponse;
import org.apache.hugegraph.pd.pulse.PDPulseSubjects;
import org.apache.hugegraph.pd.service.interceptor.GrpcAuthentication;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * 元数据信息获取与修改类
 *
 * @author zhangyingjie
 * @date 2023/9/19
 **/
@Slf4j
@GRpcService(interceptors = {GrpcAuthentication.class})
public class MetaServiceGrpcImpl extends MetaServiceImplBase implements ServiceGrpc {

    @Autowired
    private MetadataService metadataService;
    private ResponseHeader okHeader = getResponseHeader();

    /**
     *
     */
    public void getStores(NoArg request, StreamObserver<Stores> observer) {
        if (!isLeader()) {
            redirectToLeader(MetaServiceGrpc.getGetStoresMethod(), request, observer);
            return;
        }
        Stores response;
        Stores.Builder builder = Stores.newBuilder();
        try {
            response = metadataService.getStores();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(MetaServiceGrpc.getGetStoresMethod(), request, observer);
                return;
            }
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     *
     */
    public void getPartitions(NoArg request, StreamObserver<Partitions> observer) {
        if (!isLeader()) {
            redirectToLeader(MetaServiceGrpc.getGetPartitionsMethod(), request, observer);
            return;
        }
        Partitions response;
        Partitions.Builder builder = Partitions.newBuilder();
        try {
            response = metadataService.getPartitions();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(MetaServiceGrpc.getGetPartitionsMethod(), request, observer);
                return;
            }
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     *
     */
    public void getShardGroups(NoArg request, StreamObserver<ShardGroups> observer) {
        if (!isLeader()) {
            redirectToLeader(MetaServiceGrpc.getGetShardGroupsMethod(), request, observer);
            return;
        }
        ShardGroups response;
        ShardGroups.Builder builder = ShardGroups.newBuilder();
        try {
            response = metadataService.getShardGroups();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(MetaServiceGrpc.getGetShardGroupsMethod(), request, observer);
                return;
            }
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }


    /**
     *
     */
    public void getGraphSpaces(NoArg request, StreamObserver<GraphSpaces> observer) {
        if (!isLeader()) {
            redirectToLeader(MetaServiceGrpc.getGetGraphSpacesMethod(), request, observer);
            return;
        }
        GraphSpaces response;
        GraphSpaces.Builder builder = GraphSpaces.newBuilder();
        try {
            response = metadataService.getGraphSpaces();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(MetaServiceGrpc.getGetGraphSpacesMethod(), request, observer);
                return;
            }
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     *
     */
    public void getGraphs(NoArg request, StreamObserver<Graphs> observer) {
        if (!isLeader()) {
            redirectToLeader(MetaServiceGrpc.getGetGraphsMethod(), request, observer);
            return;
        }
        Graphs response;
        Graphs.Builder builder = Graphs.newBuilder();
        try {
            response = metadataService.getGraphs();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(MetaServiceGrpc.getGetGraphsMethod(), request, observer);
                return;
            }
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     *
     */
    public void updateStore(Store request, StreamObserver<VoidResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(MetaServiceGrpc.getUpdateStoreMethod(), request, observer);
            return;
        }
        VoidResponse response;
        VoidResponse.Builder builder = VoidResponse.newBuilder();
        try {
            metadataService.updateStore(request);
            response = builder.build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(MetaServiceGrpc.getUpdateStoreMethod(), request, observer);
                return;
            }
            ResponseHeader header = getResponseHeader(e);
            response = builder.setHeader(header).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     *
     */
    public void updatePartition(Partition request, StreamObserver<VoidResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(MetaServiceGrpc.getUpdatePartitionMethod(), request, observer);
            return;
        }
        VoidResponse response;
        VoidResponse.Builder builder = VoidResponse.newBuilder();
        try {
            metadataService.updatePartition(request);
            response = builder.build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(MetaServiceGrpc.getUpdatePartitionMethod(), request, observer);
                return;
            }
            ResponseHeader header = getResponseHeader(e);
            response = builder.setHeader(header).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     *
     */
    public void updateShardGroup(ShardGroup request, StreamObserver<VoidResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(MetaServiceGrpc.getUpdateShardGroupMethod(), request, observer);
            return;
        }
        VoidResponse response;
        VoidResponse.Builder builder = VoidResponse.newBuilder();
        try {
            metadataService.updateShardGroup(request);
            response = builder.build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(MetaServiceGrpc.getUpdateShardGroupMethod(), request, observer);
                return;
            }
            ResponseHeader header = getResponseHeader(e);
            response = builder.setHeader(header).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     *
     */
    public void updateGraphSpace(GraphSpace request, StreamObserver<VoidResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(MetaServiceGrpc.getUpdateGraphSpaceMethod(), request, observer);
            return;
        }
        VoidResponse response;
        VoidResponse.Builder builder = VoidResponse.newBuilder();
        try {
            metadataService.updateGraphSpace(request);
            response = builder.build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(MetaServiceGrpc.getUpdateGraphSpaceMethod(), request, observer);
                return;
            }
            ResponseHeader header = getResponseHeader(e);
            response = builder.setHeader(header).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     *
     */
    public void updateGraph(Graph request, StreamObserver<VoidResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(MetaServiceGrpc.getUpdateGraphMethod(), request, observer);
            return;
        }
        VoidResponse response;
        VoidResponse.Builder builder = VoidResponse.newBuilder();
        try {
            metadataService.updateGraph(request);
            response = builder.build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(MetaServiceGrpc.getUpdateGraphMethod(), request, observer);
                return;
            }
            ResponseHeader header = getResponseHeader(e);
            response = builder.setHeader(header).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    public void updatePeers(NoArg request, StreamObserver<VoidResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(MetaServiceGrpc.getUpdatePeersMethod(), request, observer);
            return;
        }
        VoidResponse response;
        VoidResponse.Builder builder = VoidResponse.newBuilder();
        try {
            List<String> addresses = metadataService.getPeerGrpcAddresses();
            PDPulseSubjects.notifyPeerChange(addresses);
            builder.setHeader(okHeader);
            response = builder.build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(MetaServiceGrpc.getUpdatePeersMethod(), request, observer);
                return;
            }
            ResponseHeader header = getResponseHeader(e);
            response = builder.setHeader(header).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }
}
