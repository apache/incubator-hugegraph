package org.apache.hugegraph.backend.store.raft.compress;

import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;

public class CompressStrategyManager {
    private static CompressStrategy[] compressStrategies = new CompressStrategy[5];
    private static byte DEFAULT_STRATEGY = 1;
    public static final byte SERIAL_STRATEGY = 1;
    public static final byte PARALLEL_STRATEGY = 2;

    static {
        addCompressStrategy(SERIAL_STRATEGY, new SerialCompressStrategy());
    }

    private CompressStrategyManager() {
    }

    public static void addCompressStrategy(final int idx, final CompressStrategy compressStrategy) {
        if (compressStrategies.length <= idx) {
            final CompressStrategy[] newCompressStrategies = new CompressStrategy[idx + 5];
            System.arraycopy(compressStrategies, 0, newCompressStrategies, 0, compressStrategies.length);
            compressStrategies = newCompressStrategies;
        }
        compressStrategies[idx] = compressStrategy;
    }

    public static CompressStrategy getDefault() {
        return compressStrategies[DEFAULT_STRATEGY];
    }

    public static void init(final HugeConfig config) {
        if (config.get(CoreOptions.RAFT_SNAPSHOT_PARALLEL_COMPRESS)) {
            // add parallel compress strategy
            if (compressStrategies[PARALLEL_STRATEGY] == null) {
                final CompressStrategy compressStrategy = new ParallelCompressStrategy(
                    config.get(CoreOptions.RAFT_SNAPSHOT_COMPRESS_THREADS),
                    config.get(CoreOptions.RAFT_SNAPSHOT_DECOMPRESS_THREADS));
                CompressStrategyManager.addCompressStrategy(CompressStrategyManager.PARALLEL_STRATEGY, compressStrategy);
                DEFAULT_STRATEGY = PARALLEL_STRATEGY;
            }
        }
    }
}
