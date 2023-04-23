package com.baidu.hugegraph.store.cli.scan;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.baidu.hugegraph.store.grpc.GraphStoreGrpc;
import com.baidu.hugegraph.store.grpc.GraphStoreGrpc.GraphStoreStub;
import com.baidu.hugegraph.store.grpc.Graphpb.ScanPartitionRequest;
import com.baidu.hugegraph.store.grpc.Graphpb.ScanPartitionRequest.Reply;
import com.baidu.hugegraph.store.grpc.Graphpb.ScanResponse;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * @author zhangyingjie
 * @date 2022/11/1
 **/

@Slf4j
public class GrpcShardScanner {
    private volatile boolean closed = false;
    private AtomicInteger sum = new AtomicInteger();
    private ConcurrentHashMap<Long, StreamObserver<ScanPartitionRequest>>
            observers = new ConcurrentHashMap<>();

    public void getData() {
        ExecutorService service = new ThreadPoolExecutor(500, Integer.MAX_VALUE,
                                                         0L,
                                                         TimeUnit.MILLISECONDS,
                                                         new LinkedBlockingQueue<>());
        long start = System.currentTimeMillis();

        String[] addresses = new String[]{"10.14.139.71:8500",
                                          "10.14.139.70:8500",
                                          "10.14.139.69:8500"};
        int pSize = 72;
        int size = pSize * addresses.length;
        CountDownLatch latch = new CountDownLatch(size);
        for (int j = 0; j < pSize; j++) {
            for (int i = 0; i < addresses.length; i++) {
                String address = addresses[i];
                int finalJ = j;
                service.execute(() -> getData(finalJ, latch, address));
            }
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        long cost = end - start;
        log.info("all rows are: {}, cost: {},avg: {}", sum.get(),
                 cost, sum.get() / cost * 1000);
    }

    public void getData(int pId, CountDownLatch latch, String address) {
        try {
            ScanPartitionRequest.Builder builder =
                    ScanPartitionRequest.newBuilder();
            ScanPartitionRequest.Request.Builder srb =
                    ScanPartitionRequest.Request.newBuilder();
            ScanPartitionRequest.Request request =
                    srb.setGraphName("DEFAULT/hugegraph2/g")
                       .setScanType(
                               ScanPartitionRequest.ScanType.SCAN_EDGE)
                       .setTable("g+oe").setBoundary(0x10)
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
                                // String print = JsonFormat.printer().print
                                // (value);
                                // System.out.println(print);
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
                            log.warn("调用grpc接口发生错误", t);
                            latch.countDown();
                        }

                        @Override
                        public void onCompleted() {
                            long time = System.currentTimeMillis() - start;
                            log.info("scan id : {}, complete: {} ,time:{}",
                                     pId, count.get(), time);
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
        } finally {

        }
    }

    public void getDataSingle() {
        CountDownLatch latch = new CountDownLatch(1);
        new Thread(() -> getData(58, latch, "10.14.139.71:8500")).start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        log.info("all rows are: {}", sum.get());
    }

}
