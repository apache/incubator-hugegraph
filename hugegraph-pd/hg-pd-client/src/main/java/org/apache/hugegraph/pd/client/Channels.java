package org.apache.hugegraph.pd.client;

import java.util.concurrent.ConcurrentHashMap;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class Channels {

    private static final ConcurrentHashMap<String, ManagedChannel> chs = new ConcurrentHashMap<>();

    public static ManagedChannel getChannel(String target) {

        ManagedChannel channel;
        if ((channel = chs.get(target)) == null || channel.isShutdown() || channel.isTerminated()) {
            synchronized (chs) {
                if ((channel = chs.get(target)) == null || channel.isShutdown() ||
                    channel.isTerminated()) {
                    channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
                    chs.put(target, channel);
                }
            }
        }

        return channel;
    }
}
