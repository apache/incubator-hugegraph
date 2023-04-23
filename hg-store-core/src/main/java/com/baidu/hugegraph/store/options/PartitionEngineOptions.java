package com.baidu.hugegraph.store.options;

import com.alipay.sofa.jraft.conf.Configuration;
import com.baidu.hugegraph.store.raft.RaftTaskHandler;
import lombok.Data;

import java.util.List;
import java.util.concurrent.Executor;

/**
 * Partition engine configuration
 *
 */
@Data
public class PartitionEngineOptions {
    // raft存储路径
    private String raftDataPath;
    private String raftSnapShotPath;
    private Integer groupId;
    private String raftAddress;
    private List<String> peerList;
    private Configuration conf;
    // 异步任务执行时间间隔, 单位秒
    private int taskScheduleTime = 60;
    // 分裂过程，等待数据对齐超时时间
    private long splitPartitionTimeout = 30 * 60 * 1000;
    // raft 任务处理器
    private RaftTaskHandler taskHandler;

    HgStoreEngineOptions.RaftOptions raftOptions;
}
