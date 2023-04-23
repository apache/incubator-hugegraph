package com.baidu.hugegraph.store.client.grpc;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import com.baidu.hugegraph.store.client.util.HgStoreClientConfig;
import com.baidu.hugegraph.store.term.HgPair;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.AbstractAsyncStub;
import io.grpc.stub.AbstractBlockingStub;

/**
 * @author zhangyingjie
 * @date 2023/3/28
 **/
public abstract class AbstractGrpcClient {

    private static Map<String, ManagedChannel[]> channels = new ConcurrentHashMap<>();
    private static int n = 5;
    private static int concurrency = 1 << n;
    private static AtomicLong counter = new AtomicLong(0);
    private static long limit = Long.MAX_VALUE >> 1;
    private Map<String, HgPair<ManagedChannel, AbstractBlockingStub>[]> blockingStubs = new ConcurrentHashMap<>();
    private Map<String, HgPair<ManagedChannel, AbstractAsyncStub>[]> asyncStubs = new ConcurrentHashMap<>();
    private static HgStoreClientConfig config = HgStoreClientConfig.of();

    public AbstractGrpcClient() {

    }

    public ManagedChannel[] getChannels(String target) {
        ManagedChannel[] tc;
        if ((tc = channels.get(target)) == null) {
            synchronized (channels) {
                if ((tc = channels.get(target)) == null) {
                    ManagedChannel[] value = new ManagedChannel[concurrency];
                    IntStream.range(0, concurrency).parallel().forEach(i -> value[i] = getManagedChannel(target));
                    channels.put(target, tc = value);
                }
            }
        }
        return tc;
    }

    public abstract AbstractBlockingStub getBlockingStub(ManagedChannel channel);

    public AbstractBlockingStub getBlockingStub(String target) {
        ManagedChannel[] channels = getChannels(target);
        HgPair<ManagedChannel, AbstractBlockingStub>[] pairs = blockingStubs.get(target);
        long l = counter.getAndIncrement();
        if (l >= limit) {
            counter.set(0);
        }
        int index = (int) (l & (concurrency - 1));
        if (pairs == null) {
            synchronized (blockingStubs) {
                pairs = blockingStubs.get(target);
                if (pairs == null) {
                    HgPair<ManagedChannel, AbstractBlockingStub>[] value = new HgPair[concurrency];
                    IntStream.range(0, concurrency).parallel().forEach(i -> {
                        ManagedChannel channel = channels[index];
                        AbstractBlockingStub stub = getBlockingStub(channel);
                        stub.withMaxInboundMessageSize(config.getGrpcMaxInboundMessageSize())
                            .withMaxOutboundMessageSize(config.getGrpcMaxOutboundMessageSize());
                        value[i] = new HgPair<>(channel, stub);
                        // log.info("create channel for {}",target);
                    });
                    blockingStubs.put(target, value);
                    AbstractBlockingStub stub = value[index].getValue();
                    return (AbstractBlockingStub) stub.withDeadlineAfter(config.getGrpcTimeoutSeconds(),
                                                                         TimeUnit.SECONDS);
                }
            }
        }
        return (AbstractBlockingStub) pairs[index].getValue().withDeadlineAfter(config.getGrpcTimeoutSeconds(),
                                                                                TimeUnit.SECONDS);
    }

    public AbstractAsyncStub getAsyncStub(ManagedChannel channel){
        return null;
    }

    public AbstractAsyncStub getAsyncStub(String target) {
        ManagedChannel[] channels = getChannels(target);
        HgPair<ManagedChannel, AbstractAsyncStub>[] pairs = asyncStubs.get(target);
        long l = counter.getAndIncrement();
        if (l >= limit) {
            counter.set(0);
        }
        int index = (int) (l & (concurrency - 1));
        if (pairs == null) {
            synchronized (asyncStubs) {
                pairs = asyncStubs.get(target);
                if (pairs == null) {
                    HgPair<ManagedChannel, AbstractAsyncStub>[] value = new HgPair[concurrency];
                    IntStream.range(0, concurrency).parallel().forEach(i -> {
                        ManagedChannel channel = channels[index];
                        AbstractAsyncStub stub = getAsyncStub(channel);
                        stub.withMaxInboundMessageSize(config.getGrpcMaxInboundMessageSize())
                            .withMaxOutboundMessageSize(config.getGrpcMaxOutboundMessageSize());
                        value[i] = new HgPair<>(channel, stub);
                        // log.info("create channel for {}",target);
                    });
                    asyncStubs.put(target, value);
                    AbstractAsyncStub stub = value[index].getValue();
                    return stub;
                }
            }
        }
        return pairs[index].getValue();

    }



    private ManagedChannel getManagedChannel(String target) {
        return ManagedChannelBuilder.forTarget(target).usePlaintext().build();
    }


}
