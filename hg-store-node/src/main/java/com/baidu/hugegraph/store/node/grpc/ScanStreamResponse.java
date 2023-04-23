package com.baidu.hugegraph.store.node.grpc;

import static com.baidu.hugegraph.store.node.grpc.ScanUtil.getIterator;

import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import com.baidu.hugegraph.rocksdb.access.RocksDBSession;
import com.baidu.hugegraph.rocksdb.access.ScanIterator;
import com.baidu.hugegraph.store.grpc.common.Kv;
import com.baidu.hugegraph.store.grpc.stream.KvPageRes;
import com.baidu.hugegraph.store.grpc.stream.ScanStreamReq;
import com.baidu.hugegraph.store.node.AppConfig;
import com.baidu.hugegraph.store.node.util.HgAssert;
import com.baidu.hugegraph.store.node.util.HgChannel;
import com.baidu.hugegraph.store.node.util.HgGrpc;
import com.baidu.hugegraph.store.node.util.HgStoreNodeUtil;
import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * @author lynn.bond@hotmail.com created on 2022/02/17
 * @version 3.6.0
 */
@Slf4j
public class ScanStreamResponse implements StreamObserver<ScanStreamReq> {
    private final StreamObserver<KvPageRes> responseObserver;
    private final HgStoreWrapperEx wrapper;
    private final AtomicBoolean finishFlag = new AtomicBoolean();
    private final ThreadPoolExecutor executor;
    private ScanIterator iterator;
    private long limit = 0;
    private int times = 0;
    private long pageSize = 0;
    private int total = 0;
    private String graph;
    private String table;
    private AtomicBoolean isStarted = new AtomicBoolean();
    private AtomicBoolean isStop = new AtomicBoolean(false);
    private AppConfig config;
    private int waitTime;
    private HgChannel<KvPageRes.Builder> channel;
    private static String msg = "to wait for client taking data exceeded max time: [{}] seconds,stop scanning.";

    public static ScanStreamResponse of(StreamObserver<KvPageRes> responseObserver,
                                        HgStoreWrapperEx wrapper,
                                        ThreadPoolExecutor executor, AppConfig appConfig) {
        HgAssert.isArgumentNotNull(responseObserver, "responseObserver");
        HgAssert.isArgumentNotNull(wrapper, "wrapper");
        HgAssert.isArgumentNotNull(executor, "executor");
        return new ScanStreamResponse(responseObserver, wrapper, executor, appConfig);
    }

    ScanStreamResponse(StreamObserver<KvPageRes> responseObserver,
                       HgStoreWrapperEx wrapper,
                       ThreadPoolExecutor executor, AppConfig appConfig) {
        this.responseObserver = responseObserver;
        this.wrapper = wrapper;
        this.executor = executor;
        this.config = appConfig;
        this.waitTime = this.config.getServerWaitTime();
        this.channel = HgChannel.of(waitTime);
    }

    @Override
    public void onNext(ScanStreamReq request) {
        try {
            if (request.getCloseFlag() == 1) {
                close();
            } else {
                next(request);
            }
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void onError(Throwable t) {
        this.isStop.set(true);
        this.finishServer();
        log.warn("onError from client [ graph: {} , table: {}]; Reason: {}]", graph, table, t.getMessage());
    }

    @Override
    public void onCompleted() {
        this.isStop.set(true);
        this.finishServer();
    }

    private void initIterator(ScanStreamReq request) {
        try {
            if (this.isStarted.getAndSet(true)) {
                return;
            }
            this.iterator = getIterator(request, this.wrapper);
            this.graph = request.getHeader().getGraph();
            this.table = request.getTable();
            this.limit = request.getLimit();
            this.pageSize = request.getPageSize();
            if (this.pageSize <= 0) {
                log.warn("As page-Size is less than or equals 0, no data will be send to the client.");
            }
            /*** Start scanning loop ***/
            Runnable scanning = () ->
            {
                // log.debug("Start scanning, graph = {}, table= {}, limit = " +
                //          "{}, page size = {}", this.graph, this.table, this.limit,
                //          this.pageSize);
                KvPageRes.Builder dataBuilder = KvPageRes.newBuilder();
                Kv.Builder kvBuilder = Kv.newBuilder();
                int pageCount = 0;
                try {
                    while (iterator.hasNext()) {
                        if (limit > 0 && ++this.total > limit) {
                            break;
                        }
                        if (++pageCount > pageSize) {
                            long start = System.currentTimeMillis();
                            if (!this.channel.send(dataBuilder)) {
                                if (System.currentTimeMillis() - start >= waitTime * 1000) {
                                    log.warn(msg, waitTime);
                                    this.timeoutSever();
                                }
                                return;
                            }
                            if (this.isStop.get()) {
                                return;
                            }
                            pageCount = 1;
                            dataBuilder = KvPageRes.newBuilder();
                        }
                        dataBuilder.addData(toKv(kvBuilder, iterator.next(), iterator.position()));
                    }
                    this.channel.send(dataBuilder);
                } catch (Throwable t) {
                    String msg = "an exception occurred while scanning data:";
                    StatusRuntimeException ex = HgGrpc.toErr(Status.INTERNAL, msg + t.getMessage(), t);
                    responseObserver.onError(ex);
                } finally {
                    try {
                        this.iterator.close();
                        this.channel.close();
                    } catch (Exception e) {

                    }
                }

            };
            this.executor.execute(scanning);
        } catch (Exception e) {
            StatusRuntimeException ex = HgGrpc.toErr(Status.INTERNAL, null, e);
            responseObserver.onError(ex);
            try {
                this.iterator.close();
                this.channel.close();
            } catch (Exception exception) {

            }
        } finally {

        }

        /*** Scanning loop end ***/
    }

    private Kv toKv(Kv.Builder kvBuilder, RocksDBSession.BackendColumn col,
                    byte[] position) {
        return kvBuilder
                .setKey(ByteString.copyFrom(col.name))
                .setValue(ByteString.copyFrom(col.value))
                .setCode(HgStoreNodeUtil.toInt(position))
                .build();
    }

    private void close() {
        this.isStop.set(true);
        this.channel.close();
        if (!this.finishFlag.get()) {
            responseObserver.onNext(KvPageRes.newBuilder()
                                             .addAllData(Collections.EMPTY_LIST)
                                             .setOver(true)
                                             .setTimes(++times)
                                             .build()
            );
        }

        this.finishServer();
    }

    private void next(ScanStreamReq request) {
        this.initIterator(request);
        KvPageRes.Builder resBuilder;

        try {
            resBuilder = this.channel.receive();
            times++;
        } catch (Exception e) {
            String msg = "failed to poll a page of data, cause by:";
            log.error(msg, e);
            responseObserver.onError(HgGrpc.toErr(msg + e.getMessage()));
            return;
        }
        boolean isOver = false;
        if (resBuilder == null || resBuilder.getDataList() == null || resBuilder.getDataList().isEmpty()) {
            isOver = true;
            resBuilder = KvPageRes.newBuilder().addAllData(Collections.EMPTY_LIST);
        }
        if (!this.finishFlag.get()) {
            responseObserver.onNext(resBuilder.setOver(isOver).setTimes(times).build());
        }
        if (isOver) {
            this.finishServer();
        }

    }

    private void finishServer() {
        if (!this.finishFlag.getAndSet(true)) {
            responseObserver.onCompleted();
        }
    }

    private void timeoutSever() {
        if (!this.finishFlag.getAndSet(true)) {
            String msg = "server wait time exceeds the threshold[" + waitTime +
                         "] seconds.";
            responseObserver.onError(
                    HgGrpc.toErr(Status.Code.DEADLINE_EXCEEDED, msg));
        }
    }

}