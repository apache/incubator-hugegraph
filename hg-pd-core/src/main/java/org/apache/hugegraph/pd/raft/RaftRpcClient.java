package org.apache.hugegraph.pd.raft;

import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import com.alipay.sofa.jraft.rpc.InvokeContext;
import com.alipay.sofa.jraft.rpc.RaftRpcFactory;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
public class RaftRpcClient {
    protected volatile RpcClient rpcClient;
    private RpcOptions rpcOptions;

    public synchronized boolean init(final RpcOptions rpcOptions) {
        this.rpcOptions = rpcOptions;
        final RaftRpcFactory factory = RpcFactoryHelper.rpcFactory();
        this.rpcClient = factory.createRpcClient(factory.defaultJRaftClientConfigHelper(this.rpcOptions));
        return this.rpcClient.init(null);
    }

    /**
     * 请求快照
     */
    public CompletableFuture<RaftRpcProcessor.GetMemberResponse>
    getGrpcAddress(final String address) {
        RaftRpcProcessor.GetMemberRequest request = new RaftRpcProcessor.GetMemberRequest();
        FutureClosureAdapter<RaftRpcProcessor.GetMemberResponse> response = new FutureClosureAdapter<>();
        internalCallAsyncWithRpc(JRaftUtils.getEndPoint(address), request, response);
        return response.future;
    }

    private <V> void internalCallAsyncWithRpc(final Endpoint endpoint, final RaftRpcProcessor.BaseRequest request,
                                              final FutureClosureAdapter<V> closure) {
        final InvokeContext invokeCtx = null;
        final InvokeCallback invokeCallback = new InvokeCallback() {

            @Override
            public void complete(final Object result, final Throwable err) {
                if (err == null) {
                    final RaftRpcProcessor.BaseResponse response = (RaftRpcProcessor.BaseResponse) result;
                    closure.setResponse((V) response);
                } else {
                    closure.failure(err);
                    closure.run(new Status(-1, err.getMessage()));
                }
            }
        };

        try {
            this.rpcClient.invokeAsync(endpoint, request, invokeCtx, invokeCallback, this.rpcOptions.getRpcDefaultTimeout());
        } catch (final Throwable t) {
            log.error("failed to call rpc to {}. {}", endpoint, t.getMessage());
            closure.failure(t);
            closure.run(new Status(-1, t.getMessage()));
        }
    }
}
