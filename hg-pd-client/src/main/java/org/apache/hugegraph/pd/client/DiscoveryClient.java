package org.apache.hugegraph.pd.client;

import java.io.Closeable;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import org.apache.hugegraph.pd.grpc.discovery.DiscoveryServiceGrpc;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfo;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfos;
import org.apache.hugegraph.pd.grpc.discovery.Query;
import org.apache.hugegraph.pd.grpc.discovery.RegisterInfo;

import lombok.extern.slf4j.Slf4j;

/**
 * @author zhangyingjie
 * @date 2021/12/20
 **/
@Slf4j
public abstract class DiscoveryClient extends BaseClient implements Closeable, Discoverable {

    protected int period; //心跳周期
    ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private Timer timer = new Timer("hg-pd-c-serverHeartbeat", true);
    private long registerTimeout = 30000;
    private long lockTimeout = 5;
    private TimerTask task = new TimerTask() {
        @Override
        public void run() {
            boolean locked = false;
            try {
                locked = readWriteLock.readLock().tryLock(lockTimeout, TimeUnit.SECONDS);
                if (locked) {
                    NodeInfo nodeInfo = getRegisterNode();
                    RegisterInfo register;
                    register = getLeaderInvoker().blockingCall(DiscoveryServiceGrpc.getRegisterMethod(),
                                                               nodeInfo, registerTimeout);
                    Consumer<RegisterInfo> consumer = getRegisterConsumer();
                    if (consumer != null) {
                        try {
                            consumer.accept(register);
                        } catch (Exception e) {
                            log.warn("run consumer when heartbeat with error:", e);
                        }
                    }
                }
            } catch (Exception e) {
                log.error("register with error:", e);
            } finally {
                if (locked) {
                    readWriteLock.readLock().unlock();
                }
            }
        }
    };

    public DiscoveryClient(int delay, PDConfig conf) {
        super(conf, DiscoveryServiceGrpc::newStub, DiscoveryServiceGrpc::newBlockingStub);
        this.period = delay;
        if (this.period > 60000) {
            this.registerTimeout = this.period / 2;
        }
    }


    /***
     * 获取注册节点信息
     * @param query
     * @return
     */
    @Override
    public NodeInfos getNodeInfos(Query query) {
        this.readWriteLock.readLock().lock();
        NodeInfos nodes = null;
        try {
            nodes = getLeaderInvoker().blockingCall(DiscoveryServiceGrpc.getGetNodesMethod(), query);
        } catch (Exception e) {
            log.error("Failed to invoke [ getNodeInfos ], query: {} ", query, e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }
        return nodes;
    }

    /***
     * 启动心跳任务
     */
    @Override
    public void scheduleTask() {
        timer.scheduleAtFixedRate(task, 0, period);
    }

    abstract NodeInfo getRegisterNode();

    abstract Consumer<RegisterInfo> getRegisterConsumer();

    @Override
    public void cancelTask() {
        this.timer.cancel();
    }

    @Override
    public void onLeaderChanged(String leader) {
    }

    @Override
    public void close() {
        this.timer.cancel();
        readWriteLock.writeLock().lock();
        try {
            super.close();
        } catch (Exception e) {
            log.info("Close channel with error : {}.", e);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }
}
