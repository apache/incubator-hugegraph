package org.apache.hugegraph.pd.raft;

import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import com.alipay.sofa.jraft.rpc.RpcServer;
import lombok.Data;

import java.io.Serializable;

public class RaftRpcProcessor<T extends RaftRpcProcessor.BaseRequest> implements RpcProcessor<T> {


    public static void registerProcessor(final RpcServer rpcServer, RaftEngine raftEngine) {
        rpcServer.registerProcessor(new RaftRpcProcessor<>(GetMemberRequest.class, raftEngine));
    }

    private final Class<?> requestClass;
    private RaftEngine raftEngine;

    public RaftRpcProcessor(Class<?> requestClass, RaftEngine raftEngine) {
        this.requestClass = requestClass;
        this.raftEngine = raftEngine;
    }

    @Override
    public void handleRequest(RpcContext rpcCtx, T request) {
        switch (request.magic()) {
            case BaseRequest.GET_GRPC_ADDRESS: {
                rpcCtx.sendResponse(getGrpcAddress());
                break;
            }

            default:
        }
    }

    @Override
    public String interest() {
        return this.requestClass.getName();
    }

    private GetMemberResponse getGrpcAddress(){
        GetMemberResponse rep = new GetMemberResponse();
        rep.setGrpcAddress(raftEngine.getConfig().getGrpcAddress());
        rep.setClusterId(raftEngine.getConfig().getClusterId());
        rep.setDatePath(raftEngine.getConfig().getDataPath());
        rep.setRaftAddress(raftEngine.getConfig().getAddress());
        rep.setRestAddress(raftEngine.getConfig().getHost() + ":" + raftEngine.getConfig().getPort());
        rep.setStatus(Status.OK);
        return rep;
    }

    public abstract static class BaseRequest implements Serializable {
        public static final byte GET_GRPC_ADDRESS = 0x01;

        public abstract byte magic();
    }

    @Data
    public abstract static class BaseResponse implements Serializable {
        private Status status;

    }

    @Data
    public static class GetMemberRequest extends BaseRequest {
        @Override
        public byte magic() {
            return GET_GRPC_ADDRESS;
        }
    }

    @Data
    public static class GetMemberResponse extends BaseResponse {
        private long clusterId;
        private String raftAddress;
        private String grpcAddress;
        private String datePath;
        private String restAddress;
    }

    public enum Status implements Serializable{
        UNKNOWN(-1, "unknown"),
        OK(0, "ok"),
        COMPLETE(0, "Transmission completed"),
        INCOMPLETE(1, "Incomplete transmission"),
        NO_PARTITION(10, "Partition not found"),
        IO_ERROR(11, "io error"),
        EXCEPTION(12, "exception"),
        ABORT(100, "Transmission aborted");

        private int code;
        private String msg;

        Status(int code, String msg) {
            this.code = code;
            this.msg = msg;
        }

        public int getCode(){
            return this.code;
        }

        public Status setMsg(String msg){
            this.msg = msg;
            return this;
        }

        public boolean isOK(){
            return this.code == 0;
        }
    }
}
