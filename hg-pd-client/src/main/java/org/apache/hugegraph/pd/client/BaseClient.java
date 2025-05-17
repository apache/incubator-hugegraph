package org.apache.hugegraph.pd.client;

import java.io.Closeable;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.hugegraph.pd.client.listener.LeaderChangeListener;
import org.apache.hugegraph.pd.client.rpc.AnyInvoker;
import org.apache.hugegraph.pd.client.rpc.ConnectionManager;
import org.apache.hugegraph.pd.client.rpc.ConnectionManagers;
import org.apache.hugegraph.pd.client.rpc.Invoker;
import org.apache.hugegraph.pd.client.rpc.LeaderInvoker;
import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.common.ErrorType;
import org.apache.hugegraph.pd.grpc.common.Errors;
import org.apache.hugegraph.pd.grpc.common.RequestHeader;
import org.apache.hugegraph.pd.grpc.common.ResponseHeader;

import io.grpc.Channel;
import io.grpc.MethodDescriptor;
import io.grpc.stub.AbstractBlockingStub;
import io.grpc.stub.AbstractStub;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author lynn.bond@hotmail.com on 2023/12/20
 */
@Slf4j
public abstract class BaseClient implements Closeable, LeaderChangeListener {

    public static final ResponseHeader OK_HEADER =
            ResponseHeader.newBuilder().setError(Errors.newBuilder().setType(ErrorType.OK)).build();
    protected final RequestHeader header = RequestHeader.getDefaultInstance();
    @Getter
    private final PDConfig config;
    @Getter
    private final ConnectionManager cm;
    @Getter
    private final Invoker leaderInvoker;
    @Getter
    private final Invoker anyInvoker;
    private final Function<Channel, AbstractStub> asCreator;
    private final Function<Channel, AbstractBlockingStub> bsCreator;

    protected BaseClient(PDConfig pdConfig, Function<Channel, AbstractStub> asCreator,
                         Function<Channel, AbstractBlockingStub> bsCreator) {
        this.config = pdConfig;
        this.cm = ConnectionManagers.getInstance().add(pdConfig);
        this.cm.addClient(this);
        this.asCreator = asCreator;
        this.bsCreator = bsCreator;
        this.leaderInvoker = new LeaderInvoker(this.cm, asCreator, bsCreator);
        this.anyInvoker = new AnyInvoker(this.cm, asCreator, bsCreator);
    }

    public ResponseHeader createErrorHeader(int errorCode, String errorMsg) {
        return ResponseHeader.newBuilder()
                             .setError(Errors.newBuilder().setTypeValue(errorCode).setMessage(errorMsg))
                             .build();
    }

    public void handleErrors(ResponseHeader header) throws PDException {
        Errors error = header.getError();
        if (header.hasError() && error.getType() != ErrorType.OK) {
            throw new PDException(error.getTypeValue(),
                                  String.format("PD request error, error code = %d, msg = %s",
                                                Integer.valueOf(error.getTypeValue()), error.getMessage()));
        }
    }

    public String getLeaderAddress() {
        return this.cm.getLeader();
    }

    protected <ReqT, RespT> RespT blockingUnaryCall(MethodDescriptor<ReqT, RespT> method, ReqT req) throws
                                                                                                    PDException {
        return this.leaderInvoker.blockingCall(method, req);
    }

    protected <ReqT, RespT> RespT blockingUnaryCall(MethodDescriptor<ReqT, RespT> method, ReqT req,
                                                    long timeout) throws PDException {
        return this.leaderInvoker.blockingCall(method, req, timeout);
    }

    protected <ReqT, RespT> KVPair<Boolean, RespT> concurrentBlockingUnaryCall(
            MethodDescriptor<ReqT, RespT> method, ReqT req, Predicate<RespT> predicate) throws PDException {
        RespT t = this.anyInvoker.blockingCall(method, req, predicate);
        return new KVPair(Boolean.valueOf((t != null)), t);
    }

    public void close() {
        this.cm.removeClient(this);
    }
}
