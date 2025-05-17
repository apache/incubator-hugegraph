package org.apache.hugegraph.pd.service;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;

import org.apache.hugegraph.pd.RegistryService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.common.PDRuntimeException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.common.ErrorType;
import org.apache.hugegraph.pd.grpc.common.Errors;
import org.apache.hugegraph.pd.grpc.common.ResponseHeader;
import org.apache.hugegraph.pd.grpc.discovery.DiscoveryServiceGrpc;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfo;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfos;
import org.apache.hugegraph.pd.grpc.discovery.Query;
import org.apache.hugegraph.pd.grpc.discovery.RegisterInfo;
import org.apache.hugegraph.pd.license.LicenseVerifierService;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.apache.hugegraph.pd.service.interceptor.GrpcAuthentication;

import lombok.extern.slf4j.Slf4j;

/**
 * @author zhangyingjie
 * @date 2021/12/20
 **/
@Slf4j
@GRpcService(interceptors = {GrpcAuthentication.class})
public class DiscoveryService extends DiscoveryServiceGrpc.DiscoveryServiceImplBase implements ServiceGrpc {

    private static final String CORES = "cores";
    private static AtomicLong id = new AtomicLong();
    private RegistryService register = null;
    private LicenseVerifierService licenseVerifierService;
    @Autowired
    private PDConfig pdConfig;

    @PostConstruct
    public void init() {
        RaftEngine.getInstance().init(pdConfig.getRaft());
        RaftEngine.getInstance().addStateListener(this);
        register = new RegistryService(pdConfig);
        licenseVerifierService = new LicenseVerifierService(pdConfig);
    }

    @Override
    public void register(NodeInfo request, io.grpc.stub.StreamObserver<RegisterInfo> observer) {
        if (!isLeader()) {
            redirectToLeader(DiscoveryServiceGrpc.getRegisterMethod(), request, observer);
            return;
        }
        int outTimes = pdConfig.getDiscovery().getHeartbeatOutTimes();
        RegisterInfo registerInfo;
        try {
            if (request.getAppName().equals("hg")) {
                Query queryRequest = Query.newBuilder().setAppName(request.getAppName())
                                          .setVersion(request.getVersion()).build();
                NodeInfos nodes = register.getNodes(queryRequest);
                String address = request.getAddress();
                int nodeCount = nodes.getInfoCount() + 1;
                for (NodeInfo node : nodes.getInfoList()) {
                    if (node.getAddress().equals(address)) {
                        nodeCount = nodes.getInfoCount();
                        break;
                    }
                }
                Map<String, String> labelsMap = request.getLabelsMap();
                String coreCount = labelsMap.get(CORES);
                if (StringUtils.isEmpty(coreCount)) {
                    throw new PDException(-1, "core count can not be null");
                }
                int core = Integer.parseInt(coreCount);
                licenseVerifierService.verify(core, nodeCount);
            }
            register.register(request, outTimes);
            String valueId = request.getId();
            registerInfo = RegisterInfo.newBuilder().setNodeInfo(NodeInfo.newBuilder().setId(
                    "0".equals(valueId) ? String.valueOf(id.incrementAndGet()) : valueId).build()).build();

        } catch (PDException e) {
            registerInfo = RegisterInfo.newBuilder().setHeader(getResponseHeader(e)).build();
            log.debug("registerStore exception: ", e);
        } catch (PDRuntimeException ex) {
            Errors error = Errors.newBuilder().setTypeValue(ex.getErrorCode())
                                 .setMessage(ex.getMessage()).build();
            ResponseHeader header = ResponseHeader.newBuilder().setError(error).build();
            registerInfo = RegisterInfo.newBuilder().setHeader(header).build();
            log.debug("registerStore exception: ", ex);
        } catch (Exception e) {
            Errors error = Errors.newBuilder().setTypeValue(ErrorType.UNKNOWN.getNumber())
                                 .setMessage(e.getMessage()).build();
            ResponseHeader header = ResponseHeader.newBuilder().setError(error).build();
            registerInfo = RegisterInfo.newBuilder().setHeader(header).build();
        }
        observer.onNext(registerInfo);
        observer.onCompleted();
    }

    public void getNodes(Query request, io.grpc.stub.StreamObserver<NodeInfos> responseObserver) {
        if (!isLeader()) {
            redirectToLeader(DiscoveryServiceGrpc.getGetNodesMethod(), request, responseObserver);
            return;
        }
        responseObserver.onNext(register.getNodes(request));
        responseObserver.onCompleted();
    }

    @Override
    public void onRaftLeaderChanged() {

    }
}
