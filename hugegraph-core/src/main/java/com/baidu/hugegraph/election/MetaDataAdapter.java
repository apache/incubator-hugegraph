package com.baidu.hugegraph.election;

import java.util.Optional;

public interface MetaDataAdapter {

    boolean postDelyIfPresent(MetaData metaData, long delySecond);

    Optional<MetaData> queryDelay(long delySecond);

    Optional<MetaData> query();
}
