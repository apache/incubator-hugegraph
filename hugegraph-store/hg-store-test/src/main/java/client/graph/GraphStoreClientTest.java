/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package client.graph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.store.grpc.GraphStoreGrpc;
import org.apache.hugegraph.store.grpc.GraphStoreGrpc.GraphStoreStub;
import org.apache.hugegraph.store.grpc.Graphpb.ScanPartitionRequest;
import org.apache.hugegraph.store.grpc.Graphpb.ScanPartitionRequest.Reply;
import org.apache.hugegraph.store.grpc.Graphpb.ScanResponse;
import org.junit.Test;

import com.google.protobuf.util.JsonFormat;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GraphStoreClientTest {

    private final AtomicInteger sum = new AtomicInteger();
    ConcurrentHashMap<Long, StreamObserver<ScanPartitionRequest>> observers =
            new ConcurrentHashMap<>();

    @Test
    public void getData() {
        long start = System.currentTimeMillis();
        String[] addresses = new String[]{"10.14.139.71:8500",
                                          "10.14.139.70:8500",
                                          "10.14.139.69:8500"};
        Arrays.stream(addresses).parallel().forEach(address -> {
            int size = 72;
            CountDownLatch latch = new CountDownLatch(size);

            for (int i = 0; i < size; i++) {
                int finalI = i;
                new Thread(() -> getData(finalI, latch, address)).start();
            }
            try {
                latch.await();

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        long end = System.currentTimeMillis();
        System.out.println(
                "all rows are: " + sum.get() + ", end: " + (end - start));
    }

    public void getData(int pId, CountDownLatch latch, String address) {
        try {
            ScanPartitionRequest.Builder builder =
                    ScanPartitionRequest.newBuilder();
            ScanPartitionRequest.Request.Builder srb =
                    ScanPartitionRequest.Request.newBuilder();
            ArrayList<Long> properties = new ArrayList<>() {{
                add(2L);
                add(3L);
            }};
            ScanPartitionRequest.Request request =
                    srb.setGraphName("DEFAULT/hugegraph0/g")
                       .setScanType(
                               ScanPartitionRequest.ScanType.SCAN_VERTEX).setBoundary(0x10)
                       // .addAllProperties(properties)
                       // .setCondition("element.id().asString().equals
                       // ('1:marko1')")
                       .setPartitionId(pId).build();
            ManagedChannel c =
                    ManagedChannelBuilder.forTarget(address)
                                         .usePlaintext().build();
            int maxSize = 1024 * 1024 * 1024;
            GraphStoreStub stub;
            stub = GraphStoreGrpc.newStub(c)
                                 .withMaxInboundMessageSize(maxSize)
                                 .withMaxOutboundMessageSize(maxSize);

            AtomicInteger count = new AtomicInteger();
            long start = System.currentTimeMillis();
            long id = Thread.currentThread().getId();
            StreamObserver<ScanResponse> ro =
                    new StreamObserver<ScanResponse>() {
                        @Override
                        public void onNext(ScanResponse value) {
                            try {
                                int edgeSize = value.getEdgeCount();
                                int vertexSize = value.getVertexCount();
                                if (request.getScanType().equals(
                                        ScanPartitionRequest.ScanType.SCAN_VERTEX)) {
                                    count.getAndAdd(vertexSize);
                                } else {
                                    count.getAndAdd(edgeSize);
                                }
                                String print = JsonFormat.printer().print
                                        (value);
                                System.out.println(print);
                                ScanPartitionRequest.Builder builder =
                                        ScanPartitionRequest.newBuilder();
                                builder.setScanRequest(request);
                                Reply.Builder reply = Reply.newBuilder();
                                reply.setSeqNo(1);
                                builder.setReplyRequest(reply);
                                observers.get(id).onNext(builder.build());

                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }

                        @Override
                        public void onError(Throwable t) {
                            System.out.println(t);
                            latch.countDown();
                        }

                        @Override
                        public void onCompleted() {
                            long time = System.currentTimeMillis() - start;
                            System.out.println(
                                    "scan id :" + pId + ", complete: " +
                                    count.get() + ",time: " + time);
                            sum.addAndGet(count.get());
                            latch.countDown();
                        }
                    };
            StreamObserver<ScanPartitionRequest> observer =
                    stub.scanPartition(ro);
            observers.put(id, observer);
            builder.setScanRequest(request);
            observer.onNext(builder.build());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void getDataSingle() {
        CountDownLatch latch = new CountDownLatch(1);
        new Thread(() -> getData(58, latch, "10.14.139.71:8500")).start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("all rows are: " + sum.get());
    }

    @Test
    public void getNativeDataSingle() {
        CountDownLatch latch = new CountDownLatch(1);
        new Thread(() -> getData(0, latch, "127.0.0.1:8500")).start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("all rows are: " + sum.get());
    }
}
