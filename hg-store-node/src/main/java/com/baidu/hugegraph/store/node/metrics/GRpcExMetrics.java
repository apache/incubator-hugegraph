package com.baidu.hugegraph.store.node.metrics;

import com.baidu.hugegraph.store.node.grpc.GRpcServerConfig;
import com.baidu.hugegraph.store.node.util.HgExecutorUtil;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author lynn.bond@hotmail.com on 2022/3/8
 */
public class GRpcExMetrics {
    public final static String PREFIX = "grpc";
    private static MeterRegistry registry;
    private final static ExecutorWrapper wrapper = new ExecutorWrapper();

    private GRpcExMetrics() {}

    public synchronized static void init(MeterRegistry meterRegistry) {
        if (registry == null) {
            registry = meterRegistry;
            registerMeters();
        }
    }

    private static void registerMeters() {
        registerExecutor();

    }

    private static void registerExecutor(){

        Gauge.builder(PREFIX + ".executor.pool.size",wrapper,(e)->e.getPoolSize())
                .description("The current number of threads in the pool.")
                .register(registry);

        Gauge.builder(PREFIX + ".executor.core.pool.size",wrapper,(e)->e.getCorePoolSize())
                .description("The largest number of threads that have ever simultaneously been in the pool.")
                .register(registry);

        Gauge.builder(PREFIX + ".executor.active.count",wrapper,(e)->e.getActiveCount())
                .description("The approximate number of threads that are actively executing tasks.")
                .register(registry);
    }

    private static class ExecutorWrapper{
        ThreadPoolExecutor pool;

        void init(){
            if(this.pool==null){
                pool=HgExecutorUtil.getThreadPoolExecutor(GRpcServerConfig.EXECUTOR_NAME);
            }
        }

        double getPoolSize(){
            init();
            return this.pool==null?0d:this.pool.getPoolSize();
        }

        int getCorePoolSize(){
            init();
            return this.pool==null?0:this.pool.getCorePoolSize();
        }

        int getActiveCount(){
            init();
            return this.pool==null?0:this.pool.getActiveCount();
        }

    }

}
