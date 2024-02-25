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

package org.apache.hugegraph.store.client.grpc;

import static org.apache.hugegraph.store.client.grpc.KvBatchUtil.getHeader;

import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.client.HgStoreNodeSession;
import org.apache.hugegraph.store.grpc.common.GraphMethod;
import org.apache.hugegraph.store.grpc.common.ScanMethod;
import org.apache.hugegraph.store.grpc.common.TableMethod;
import org.apache.hugegraph.store.grpc.session.Agg;
import org.apache.hugegraph.store.grpc.session.BatchEntry;
import org.apache.hugegraph.store.grpc.session.BatchGetReq;
import org.apache.hugegraph.store.grpc.session.BatchReq;
import org.apache.hugegraph.store.grpc.session.BatchWriteReq;
import org.apache.hugegraph.store.grpc.session.CleanReq;
import org.apache.hugegraph.store.grpc.session.FeedbackRes;
import org.apache.hugegraph.store.grpc.session.GetReq;
import org.apache.hugegraph.store.grpc.session.GraphReq;
import org.apache.hugegraph.store.grpc.session.HgStoreSessionGrpc;
import org.apache.hugegraph.store.grpc.session.HgStoreSessionGrpc.HgStoreSessionBlockingStub;
import org.apache.hugegraph.store.grpc.session.TableReq;
import org.apache.hugegraph.store.grpc.stream.ScanStreamReq;

import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import lombok.extern.slf4j.Slf4j;

/**
 * created on 2021/11/18
 *
 * @version 0.5.0
 */
@Slf4j
@ThreadSafe
class GrpcStoreSessionClient extends AbstractGrpcClient {

    @Override
    public HgStoreSessionBlockingStub getBlockingStub(ManagedChannel channel) {
        HgStoreSessionBlockingStub stub;
        stub = HgStoreSessionGrpc.newBlockingStub(channel);
        return stub;
    }

    private HgStoreSessionBlockingStub getBlockingStub(HgStoreNodeSession nodeSession) {
        HgStoreSessionBlockingStub stub =
                (HgStoreSessionBlockingStub) getBlockingStub(
                        nodeSession.getStoreNode().getAddress());
        return stub;
    }

    FeedbackRes doGet(HgStoreNodeSession nodeSession, String table, HgOwnerKey ownerKey) {
        if (log.isDebugEnabled()) {
            log.debug("doGet: {}-{}-{}-{}", nodeSession, table, ownerKey, GetReq.newBuilder()
                                                                                .setHeader(
                                                                                        GrpcUtil.getHeader(
                                                                                                nodeSession))
                                                                                .setTk(GrpcUtil.toTk(
                                                                                        table,
                                                                                        ownerKey))
                                                                                .build());
        }
        return this.getBlockingStub(nodeSession)
                   .get2(GetReq.newBuilder()
                               .setHeader(GrpcUtil.getHeader(nodeSession))
                               .setTk(GrpcUtil.toTk(table, ownerKey))
                               .build()
                   );
    }

    FeedbackRes doClean(HgStoreNodeSession nodeSession, int partId) {
        return this.getBlockingStub(nodeSession)
                   .clean(CleanReq.newBuilder()
                                  .setHeader(GrpcUtil.getHeader(nodeSession))
                                  .setPartition(partId)
                                  .build()
                   );
    }

    FeedbackRes doBatchGet(HgStoreNodeSession nodeSession, String table, List<HgOwnerKey> keyList) {
        BatchGetReq.Builder builder = BatchGetReq.newBuilder();
        builder.setHeader(GrpcUtil.getHeader(nodeSession)).setTable(table);

        for (HgOwnerKey key : keyList) {
            builder.addKey(GrpcUtil.toKey(key));
        }

        if (log.isDebugEnabled()) {
            log.debug("batchGet2: {}-{}-{}-{}", nodeSession, table, keyList, builder.build());
        }
        return this.getBlockingStub(nodeSession).batchGet2(builder.build());

    }

    FeedbackRes doBatch(HgStoreNodeSession nodeSession, String batchId, List<BatchEntry> entries) {
        BatchWriteReq.Builder writeReq = BatchWriteReq.newBuilder();
        writeReq.addAllEntry(entries);
        return this.getBlockingStub(nodeSession)
                   .batch(BatchReq.newBuilder()
                                  .setHeader(GrpcUtil.getHeader(nodeSession))
                                  .setWriteReq(writeReq)
                                  .setBatchId(batchId)
                                  .build()
                   );
    }

    FeedbackRes doTable(HgStoreNodeSession nodeSession, String table, TableMethod method) {
        return this.getBlockingStub(nodeSession)
                   .table(TableReq.newBuilder()
                                  .setHeader(GrpcUtil.getHeader(nodeSession))
                                  .setTableName(table)
                                  .setMethod(method)
                                  .build()
                   );
    }

    FeedbackRes doGraph(HgStoreNodeSession nodeSession, String graph, GraphMethod method) {
        return this.getBlockingStub(nodeSession)
                   .graph(GraphReq.newBuilder()
                                  .setHeader(GrpcUtil.getHeader(nodeSession))
                                  .setGraphName(graph)
                                  .setMethod(method)
                                  .build()
                   );
    }

    public long count(HgStoreNodeSession nodeSession, String table) {
        Agg agg = this.getBlockingStub(nodeSession).withDeadline(Deadline.after(24, TimeUnit.HOURS))
                      .count(ScanStreamReq.newBuilder()
                                          .setHeader(getHeader(nodeSession))
                                          .setTable(table)
                                          .setMethod(ScanMethod.ALL)
                                          .build()
                      );
        return agg.getCount();
    }
}


