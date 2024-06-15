/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.store.cmd;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hugegraph.store.meta.Partition;
import org.apache.hugegraph.store.meta.Store;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import com.alipay.sofa.jraft.rpc.InvokeContext;
import com.alipay.sofa.jraft.rpc.RaftRpcFactory;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HgCmdClient {

    private static final int MAX_RETRY_TIMES = 5;
    protected volatile RpcClient rpcClient;
    private RpcOptions rpcOptions;
    private PartitionAgent ptAgent;

    public synchronized boolean init(final RpcOptions rpcOptions, PartitionAgent ptAgent) {
        this.ptAgent = ptAgent;
        this.rpcOptions = rpcOptions;
        final RaftRpcFactory factory = RpcFactoryHelper.rpcFactory();
        this.rpcClient =
                factory.createRpcClient(factory.defaultJRaftClientConfigHelper(this.rpcOptions));
        return this.rpcClient.init(rpcOptions);
    }

    public <T> Future<T> createRaftNode(final String address, final List<Partition> partitions,
                                        final Closure done) {
        CreateRaftRequest request = new CreateRaftRequest();
        partitions.forEach(partition -> {
            request.addPartition(partition.getProtoObj());
        });

        log.info("Send to {} CreateRaftNode rpc call {} ", address, request.getPartitions().get(0));
        return internalCallAsyncWithRpc(JRaftUtils.getEndPoint(address), request, done);
    }

    public <T> Future<T> createRaftNode(final String address, final List<Partition> partitions,
                                        Configuration conf, final Closure done) {
        CreateRaftRequest request = new CreateRaftRequest();
        partitions.forEach(partition -> {
            request.addPartition(partition.getProtoObj());
        });
        request.setConf(conf);

        log.info("Send to {} CreateRaftNode rpc call {} ", address, request.getPartitions().get(0));
        return internalCallAsyncWithRpc(JRaftUtils.getEndPoint(address), request, done);
    }

    public <T> Future<T> destroyRaftNode(final String peer, final List<Partition> partitions,
                                         final Closure done) {

        DestroyRaftRequest request = new DestroyRaftRequest();
        partitions.forEach(partition -> {
            request.setPartitionId(partition.getId());
            request.addGraphName(partition.getGraphName());
        });

        log.info("Send to {} DestroyRaftNode rpc call  partitionId={} ", peer,
                 request.getPartitionId());
        return internalCallAsyncWithRpc(JRaftUtils.getEndPoint(peer), request, done);
    }

    public Store getStoreInfo(final String address) {
        GetStoreInfoRequest request = new GetStoreInfoRequest();
        request.setGraphName("");
        request.setPartitionId(0);
        GetStoreInfoResponse response = null;
        try {
            response = internalCallSyncWithRpc(JRaftUtils.getEndPoint(address), request);
        } catch (Exception e) {
            return null;
        }
        return response != null ? response.getStore() : null;
    }

    /**
     * 批量插入数据
     *
     * @param request
     * @return
     */
    public BatchPutResponse batchPut(BatchPutRequest request) {
        return (BatchPutResponse) tryInternalCallSyncWithRpc(request);
    }

    /**
     * 清理无效数据
     *
     * @param request
     * @return
     */
    public CleanDataResponse cleanData(CleanDataRequest request) {
        return (CleanDataResponse) tryInternalCallSyncWithRpc(request);
    }

    /**
     * 通过raft更新本地分区信息
     *
     * @param request
     * @return
     */
    public UpdatePartitionResponse raftUpdatePartition(UpdatePartitionRequest request) {
        return (UpdatePartitionResponse) tryInternalCallSyncWithRpc(request);
    }

    /**
     * 查找Leader，错误重试，处理Leader重定向
     *
     * @param request
     * @return
     */
    public HgCmdBase.BaseResponse tryInternalCallSyncWithRpc(HgCmdBase.BaseRequest request) {
        HgCmdBase.BaseResponse response = null;

        for (int i = 0; i < MAX_RETRY_TIMES; i++) {
            try {
                Endpoint leader = ptAgent.getPartitionLeader(request.getGraphName(),
                                                             request.getPartitionId());
                if (leader == null) {
                    log.error("get leader of graph {} - {} is null", request.getGraphName(),
                              request.getPartitionId());
                    Thread.sleep(i * 1000);
                    continue;
                }

                response = internalCallSyncWithRpc(leader, request);
                if (response != null) {
                    if (response.getStatus().isOK()) {
                        break;
                    } else if (HgCmdProcessor.Status.LEADER_REDIRECT == response.getStatus()
                               && response.partitionLeaders != null
                    ) {
                        // 当返回leader 漂移，并且partitionLeaders 不为空时，需要重新设置leader
                    } else {
                        log.error(
                                "HgCmdClient tryInternalCallSyncWithRpc error msg {} leaders is {}",
                                response.getStatus().getMsg(), response.getPartitionLeaders());
                    }
                }
//                break;
            } catch (Exception e) {
                if (i + 1 >= MAX_RETRY_TIMES) {
                    log.error("tryInternalCallSyncWithRpc Exception {}", e);
                }
            }
        }
        return response;
    }

    private <V> V internalCallSyncWithRpc(final Endpoint endpoint,
                                          final HgCmdBase.BaseRequest request)
            throws ExecutionException, InterruptedException, TimeoutException {
        FutureClosureAdapter<V> response = new FutureClosureAdapter<>();
        internalCallAsyncWithRpc(endpoint, request, response);
        try {
            return response.future.get(5000, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw e;
        }
    }

    private <V> Future<V> internalCallAsyncWithRpc(final Endpoint endpoint,
                                                   final HgCmdBase.BaseRequest request,
                                                   final Closure done) {
        final InvokeContext invokeCtx = null;
        int[] retryCount = new int[]{0};
        FutureClosureAdapter<V> response = new FutureClosureAdapter<>() {
            @Override
            public void run(Status status) {
                done.run(status);
            }
        };
        tryWithTimes(endpoint, request, response, invokeCtx, retryCount);
        return response.future;
    }

    private <V> void internalCallAsyncWithRpc(final Endpoint endpoint,
                                              final HgCmdBase.BaseRequest request,
                                              final FutureClosureAdapter<V> closure) {
        final InvokeContext invokeCtx = null;
        int[] retryCount = new int[]{0};
        tryWithTimes(endpoint, request, closure, invokeCtx, retryCount);
    }

    private <V> void tryWithTimes(Endpoint endpoint, HgCmdBase.BaseRequest request,
                                  FutureClosureAdapter<V> closure,
                                  InvokeContext invokeCtx,
                                  int[] retryCount) {
        InvokeCallback invokeCallback = (result, err) -> {
            if (err == null) {
                final HgCmdBase.BaseResponse response = (HgCmdBase.BaseResponse) result;
                closure.setResponse((V) response);
            } else {
                tryWithThrowable(endpoint, request, closure, invokeCtx, retryCount, err);
            }
        };
        try {
            this.rpcClient.invokeAsync(endpoint, request, invokeCtx, invokeCallback,
                                       this.rpcOptions.getRpcDefaultTimeout());
        } catch (final Throwable err) {
            tryWithThrowable(endpoint, request, closure, invokeCtx, retryCount, err);
        }
    }

    private <V> void tryWithThrowable(Endpoint endpoint,
                                      HgCmdBase.BaseRequest request,
                                      FutureClosureAdapter<V> closure,
                                      InvokeContext invokeCtx,
                                      int[] retryCount, Throwable err) {
        if (retryCount[0] >= MAX_RETRY_TIMES) {
            closure.failure(err);
            closure.run(new Status(-1, err.getMessage()));
        } else {
            retryCount[0]++;
            try {
                Thread.sleep(100L * retryCount[0]);
            } catch (InterruptedException e) {
                closure.run(new Status(-1, e.getMessage()));
            }
            tryWithTimes(endpoint, request, closure, invokeCtx, retryCount);
        }
    }

    public interface PartitionAgent {

        Endpoint getPartitionLeader(String graph, int partitionId);
    }
}
