package org.apache.hugegraph.pd.client.rpc;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hugegraph.pd.client.PDConfig;

import io.grpc.Channel;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class Channels {
    private static ConcurrentHashMap<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    /**
     * Retrieve a channel with a specific target. If the channel is null or shutdown, create a new one;
     *
     * @param target
     * @return
     */
    public static ManagedChannel getChannel(String target) {
        ManagedChannel channel = channels.get(target);
        if (!isValidChannel(channel)) {
            synchronized (channels) {
                channel = channels.get(target);
                if (!isValidChannel(channel)) {
                    if (channel != null) {
                        log.info("get channel {}, state:{}", channel, channel.getState(false));
                    }
                    channel = resetChannel(target, channel);
                }
            }
        }
        return channel;
    }

    private static ManagedChannel resetChannel(String target, ManagedChannel channel) {
        closeChannel(channel);
        channel = ManagedChannelBuilder.forTarget(target)
                                       .maxInboundMessageSize(PDConfig.getInboundMessageSize())
                                       .usePlaintext().build();
        channels.put(target, channel);
        log.info("Because the channel is not available, create a new one for {}", target);
        return channel;
    }

    /**
     * Validate the channel weather it is valid.
     *
     * @param channel
     * @return true if the channel is valid, otherwise false.
     */
    public static boolean isValidChannel(ManagedChannel channel) {
        if (channel == null || channel.isShutdown() || channel.isTerminated()) {
            return false;
        }
        ConnectivityState state = channel.getState(false);
        if (state == ConnectivityState.READY || state == ConnectivityState.IDLE) {
            /* Optimistic judgment for increasing the efficiency. */
            return true;
        }
        /* Trying to make a connection. */
        state = channel.getState(true);
        if (state == ConnectivityState.IDLE || state == ConnectivityState.READY) {
            return true;
        } else {
            // log.info("Channel {} is invalid, state: {}", channel, state);
            return false;
        }
    }

    /**
     * Return true if the channel io is broken and need to shut down now.
     *
     * @param throwable
     * @return
     */
    public static boolean isIoBrokenError(Throwable throwable) {
        if (throwable instanceof StatusRuntimeException) {
            StatusRuntimeException e = (StatusRuntimeException) throwable;
            return e.getStatus().getCode() == Status.Code.UNAVAILABLE;
        }

        return false;
    }

    public static boolean canNotWork(Throwable throwable) {
        if (throwable instanceof StatusRuntimeException) {
            StatusRuntimeException e = (StatusRuntimeException) throwable;
            Status.Code code = e.getStatus().getCode();
            return code == Status.Code.UNAVAILABLE || code == Status.Code.DEADLINE_EXCEEDED;
        }
        return false;
    }
    /**
     * Retrieves all channels
     *
     * @return non-null collection
     */
    public static List<ManagedChannel> getAllChannels() {
        return channels.values().stream().collect(Collectors.toList());
    }

    /**
     * Closing all channels
     *
     * @return
     */
    public static boolean closeAllChannels() {
        /* Clone the list to avoid closing the new channels. */
        List<ManagedChannel> buff = new ArrayList<>(channels.values());
        return buff.stream().parallel().allMatch(Channels::closeChannel);
    }

    /**
     * Closing a channel.
     *
     * @param channel
     * @return
     */
    public static boolean closeChannel(ManagedChannel channel) {
        if (channel == null || channel.isShutdown() || channel.isTerminated()) {
            return true;
        }
        log.info("Closing the channel: {}", channel);
        try {
            channel.shutdown().awaitTermination(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.warn("Failed to close channel, caused by InterruptedException:", e);
            //Thread.currentThread().interrupt();
            return false;
        }
        while (!channel.isShutdown()) {
            try {
                log.info("Waiting for channel to be shutdown: {}", channel);
                TimeUnit.MILLISECONDS.sleep(1000);
            } catch (InterruptedException e) {
                log.warn("Failed to close channel, caused by InterruptedException:", e);
                //Thread.currentThread().interrupt();
                return false;
            }
        }
        return true;
    }

    /**
     * Invoking shutdownNow on a channel and waiting until the timeout occurs.
     * If the channel is not a ManagedChannel, return directly with no action.
     *
     * @param channel
     * @param timeout timeout in milliseconds
     */
    public static void shutdownNow(Channel channel, long timeout) {
        if (channel == null) {
            return;
        }

        if (!(channel instanceof ManagedChannel)) {
            log.info("Channel is not a ManagedChannel, return.");
            return;
        }

        ManagedChannel managedChannel = (ManagedChannel) channel;
        if (managedChannel.isShutdown() || managedChannel.isTerminated()) {
            return;
        }

        log.info("Shutting down the channel: {}", channel);

        try {
            managedChannel.shutdownNow().awaitTermination(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.warn("Failed to shutdown channel, caused by InterruptedException:", e);
            //Thread.currentThread().interrupt();
            return;
        }

    }

    public static boolean isShutdown(ManagedChannel channel) {
        if (channel == null || channel.isShutdown() || channel.isTerminated())
            return true;
        return false;
    }
}