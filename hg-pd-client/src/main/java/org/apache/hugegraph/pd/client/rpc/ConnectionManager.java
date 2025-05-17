package org.apache.hugegraph.pd.client.rpc;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang3.StringUtils;

import org.apache.hugegraph.pd.client.BaseClient;
import org.apache.hugegraph.pd.client.ClientCache;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.client.PulseClient;
import org.apache.hugegraph.pd.client.interceptor.Authentication;
import org.apache.hugegraph.pd.client.support.PDExecutors;
import org.apache.hugegraph.pd.common.HgAssert;
import org.apache.hugegraph.pd.common.PDRuntimeException;
import org.apache.hugegraph.pd.grpc.PDGrpc;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.common.ErrorType;
import org.apache.hugegraph.pd.grpc.common.NoArg;
import org.apache.hugegraph.pd.grpc.pulse.PdInstructionResponse;
import org.apache.hugegraph.pd.grpc.pulse.PdInstructionType;
import org.apache.hugegraph.pd.grpc.pulse.PulseResponse;
import org.apache.hugegraph.pd.watch.NodeEvent;
import org.apache.hugegraph.pd.watch.Watcher;
import org.apache.hugegraph.pd.watch.WatcherImpl;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractBlockingStub;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author lynn.bond@hotmail.com on 2023/11/20
 * @version 1.0
 */
@Slf4j
@ThreadSafe
public final class ConnectionManager {

    static final long WAITING_CREATE_SECONDS = 60L;
    static final long WAITING_LEADER_SECONDS = 30L;
    static final long TIMEOUT_SECONDS = 30L;
    private static String emptyMsg = "Failed to get leader after " + WAITING_LEADER_SECONDS + " attempts";
    private static NoArg noArg = NoArg.newBuilder().build();
    @Getter
    private final PDConfig config;
    private final Set<BaseClient> clients = ConcurrentHashMap.newKeySet();
    private final ExecutorService connectExecutor =
            PDExecutors.newDiscardPool("reconnect", 8, 8, Integer.MAX_VALUE);
    private final ReentrantReadWriteLock reconnectLock = new ReentrantReadWriteLock(true);
    private final ReentrantLock resetLock = new ReentrantLock(true);
    private final AtomicReference<String> leader = new AtomicReference<>();
    private final Authentication auth;
    ReentrantReadWriteLock.ReadLock readLock = this.reconnectLock.readLock();
    ReentrantReadWriteLock.WriteLock writeLock = this.reconnectLock.writeLock();
    private volatile ManagedChannel leaderChannel = null;
    private Random random = new Random();
    private InvokeProxy proxy;
    @Getter
    private ClientCache cache;
    @Getter
    private PulseClient pulseClient;
    @Getter
    private Watcher watcher;
    @Getter
    private ConnectionClient connectionClient;

    ConnectionManager(PDConfig config) {
        this.config = config;
        String[] addresses = this.config.getServerHost().split(",");
        this.proxy = new InvokeProxy(addresses);
        this.auth = new Authentication(config.getUserName(), config.getAuthority());
    }

    public void init(PulseClient pulseClient, ConnectionClient connectionClient) {
        this.pulseClient = pulseClient;
        this.watcher = new WatcherImpl(pulseClient);
        this.watcher.watchNode(this::onLeaderChanged);
        this.cache = new ClientCache(connectionClient, this.watcher);
        this.connectionClient = connectionClient;
        this.watcher.watchPdPeers(this::onPdPeersChanged);
        this.setProxyByPd();
    }

    public void setProxyByPd() {
        try {
            if (config.isAutoGetPdServers()) {
                Pdpb.GetAllGrpcAddressesResponse response = connectionClient.getPdAddressesCache();
                if (response.getHeader().getError().getType() == ErrorType.OK) {
                    if (response.getAllowed()) {
                        this.proxy = new InvokeProxy(response.getAddressesList().toArray(new String[0]));
                        log.info("Get pd servers from cache: {}", response.getAddressesList());
                    }
                } else {
                    log.warn("Failed to get pd servers from cache, {}", response);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to get pd servers from cache, ", e);
        }
    }

    private void onPdPeersChanged(PulseResponse pulseResponse) {
        PdInstructionResponse ir = pulseResponse.getInstructionResponse();
        if (ir != null && ir.getInstructionType() == PdInstructionType.CHANGE_PEERS) {
            updatePeers(ir.getPeersList().toArray(new String[0]));
        }
    }

    public String getLeaderFromPD() {
        for (int i = 0; i < WAITING_LEADER_SECONDS; i++) {
            String next = "";
            List<String> hosts = this.proxy.getHosts();
            int hostCount = hosts.size();
            int startIndex = this.random.nextInt(hostCount);
            int endIndex = startIndex + hostCount;
            ManagedChannel channel = null;
            long start = System.currentTimeMillis();
            PDGrpc.PDBlockingStub stub = null;
            for (int j = startIndex; j < endIndex; j++) {
                try {
                    if (j >= hostCount) {
                        next = hosts.get(j - hostCount);
                    } else {
                        next = hosts.get(j);
                    }
                    start = System.currentTimeMillis();
                    channel = Channels.getChannel(next);
                    stub = PDGrpc.newBlockingStub(channel)
                                 .withDeadlineAfter(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                                 .withInterceptors(this.auth);
                    Pdpb.GetLeaderGrpcAddressResponse response = stub.getLeaderGrpcAddress(noArg);
                    pulseClient.handleErrors(response.getHeader());
                    String leader = response.getAddress();
                    if (!StringUtils.isEmpty(leader)) {
                        log.info("Get leader address: {} from {}", leader, next);
                        return leader;
                    }
                } catch (StatusRuntimeException se) {
                    if (i > 5) {
                        log.warn("Channel {} may be unavailable, state:{}, last:{} ms, option:{}, " +
                                 "exception: ",
                                 stub.getChannel(),
                                 channel.getState(false), System.currentTimeMillis() - start,
                                 stub.getCallOptions(), se.getStatus());
                    }
                } catch (Exception e) {
                    log.warn(String.format("Failed to get leader by address: %s, ", next), e);
                }
            }
            try {
                Thread.sleep(1000L);
            } catch (Exception exception) {
            }
        }
        return "";
    }

    public String getLeader() {
        try {
            return this.leader.get();
        } catch (Exception e) {
            log.error("Failed to get leader address, caused by:", e);
            return "";
        }
    }

    public long getDefaultDeadline() {
        return this.config.getGrpcTimeOut();
    }

    public void addClient(BaseClient client) {
        this.clients.add(client);
    }

    public void removeClient(BaseClient client) {
        this.clients.remove(client);
    }

    public void reconnect() {
        reconnect("", false);
    }

    public void reconnect(boolean recheck) {
        reconnect("", recheck);
    }

    public void reconnect(String leaderAddress, boolean recheck) {
        long start = System.currentTimeMillis();
        boolean locked = this.writeLock.tryLock();
        if (locked) {
            try {
                if (recheck && !Channels.isShutdown(this.leaderChannel)) {
                    return;
                }
                if (StringUtils.isEmpty(leaderAddress)) {
                    leaderAddress = getLeaderFromPD();
                    if (StringUtils.isEmpty(leaderAddress)) {
                        throw new PDRuntimeException(ErrorType.PD_RAFT_NOT_READY_VALUE, emptyMsg);
                    } else {
                        log.info("Get leader address: {}", leaderAddress);
                    }
                }
                update(leaderAddress, start, false);
            } catch (Exception e) {
                throw e;
            } finally {
                this.writeLock.unlock();
            }
        } else {
            boolean readLocked = false;
            try {
                readLocked = this.readLock.tryLock(WAITING_CREATE_SECONDS, TimeUnit.SECONDS);
            } catch (Exception e) {

            } finally {
                if (readLocked) {
                    this.readLock.unlock();
                }
            }
        }
    }

    private void update(String leaderAddress, long start, boolean blocking) {
        String currentAddress = this.leader.get();
        if (!leaderAddress.equals(currentAddress) || !Channels.isValidChannel(leaderChannel)) {
            this.leader.set(leaderAddress);
            this.leaderChannel = Channels.getChannel(leaderAddress);
            String finalLeaderAddress = leaderAddress;
            Future<?> future = this.connectExecutor.submit(() -> resetClients(finalLeaderAddress));
            if (blocking) {
                try {
                    future.get();
                } catch (Exception e) {
                    log.warn("Failed to reset clients, caused by:", e);
                }
            }
            long end = System.currentTimeMillis();
            log.info("Reset leader from {} to {} in {} ms", currentAddress, leaderAddress, end - start);
        }
    }

    public void updatePeers(String[] endpoints) {
        boolean locked = resetLock.tryLock();
        if (locked) {
            try {
                log.warn("Update PD peers to {}", Arrays.toString(endpoints));
                this.proxy = new InvokeProxy(endpoints);
                reconnect();
                log.warn("PD peers updated.");
            } finally {
                resetLock.unlock();
            }
        }
    }

    public void close() {
        PDExecutors.asyncCallback(() -> Boolean.valueOf(close(10L)), b -> {
            if (b.booleanValue()) {
                log.info("Closed all channels held by this PDConnectionManager.");
            } else {
                log.warn("Failed to close all channels held by this PDConnectionManager.");
            }
        });
    }

    public boolean close(long timeout) {
        return PDExecutors.awaitTask(this::closeAllChannels, "Close all channels",
                                     timeout).booleanValue();
    }

    private boolean closeAllChannels() {
        return this.proxy.getHosts().parallelStream().map(Channels::getChannel)
                         .allMatch(Channels::closeChannel);
    }

    public Stream<ManagedChannel> getParallelChannelStream() {
        return this.proxy.getHosts().parallelStream().map(Channels::getChannel)
                         .filter(Channels::isValidChannel);
    }

    private boolean resetClients(String leaderAddress) {
        for (BaseClient client : this.clients) {
            try {
                client.onLeaderChanged(leaderAddress);
            } catch (Exception e) {
                log.warn(String.format("Failed to let client %s reconnect, caused by:", client.getClass()),
                         e);
            }
        }
        return true;
    }

    public Channel getValidChannel() {
        return this.proxy.getHosts().stream().map(Channels::getChannel).filter(Channels::isValidChannel)
                         .findFirst().orElse(null);
    }

    public <T extends io.grpc.stub.AbstractStub<T>> T createAsyncStub(Function<Channel, T> stubCreator) {
        HgAssert.isArgumentNotNull(stubCreator, "The stub creator can't be null");
        return withAsyncParams(stubCreator.apply(getValidChannel()));
    }

    public <T extends AbstractBlockingStub<T>> T createBlockingStub(Function<Channel, T> stubCreator) {
        HgAssert.isArgumentNotNull(stubCreator, "The stub creator can't be null");
        return createBlockingStub(stubCreator, getValidChannel());
    }

    private <T extends AbstractBlockingStub<T>> T createBlockingStub(Function<Channel, T> creator,
                                                                     Channel channel) {
        return withBlockingParams(creator.apply(channel));
    }

    public <T extends io.grpc.stub.AbstractStub<T>> T withAsyncParams(T stub) {
        HgAssert.isArgumentNotNull(stub, "The stub can't be null");
        return stub.withMaxInboundMessageSize(PDConfig.getInboundMessageSize()).withInterceptors(auth);
    }

    public <T extends AbstractBlockingStub<T>> T withBlockingParams(T stub) {
        HgAssert.isArgumentNotNull(stub, "The stub can't be null");
        return stub.withMaxInboundMessageSize(PDConfig.getInboundMessageSize()).withInterceptors(auth);
    }

    public Channel getLeaderChannel() {
        if (this.leaderChannel == null || Channels.isShutdown(this.leaderChannel)) {
            reconnect(true);
        }
        return this.leaderChannel;
    }

    private void onLeaderChanged(NodeEvent response) {
        if (response.getEventType() == NodeEvent.EventType.NODE_PD_LEADER_CHANGE) {
            reconnect();
        }
    }
}