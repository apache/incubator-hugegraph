package com.baidu.hugegraph.election;

public interface Config {

    String node();

    int exceedsFailCount();

    long randomTimeoutMillisecond();

    long heartBeatIntervalSecond();

    int exceedsWorkerCount();

    long baseTimeoutMillisecond();
}
