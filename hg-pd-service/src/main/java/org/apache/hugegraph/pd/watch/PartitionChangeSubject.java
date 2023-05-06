package org.apache.hugegraph.pd.watch;

import com.baidu.hugegraph.pd.grpc.watch.WatchChangeType;
import com.baidu.hugegraph.pd.grpc.watch.WatchResponse;
import com.baidu.hugegraph.pd.grpc.watch.WatchType;

import javax.annotation.concurrent.ThreadSafe;

import static com.baidu.hugegraph.pd.common.HgAssert.isArgumentNotNull;
import static com.baidu.hugegraph.pd.common.HgAssert.isArgumentValid;

/**
 * The subject of partition change.
 * @author lynn.bond@hotmail.com created on 2021/11/5
 */
@ThreadSafe
final class PartitionChangeSubject extends AbstractWatchSubject {

    PartitionChangeSubject() {
        super(WatchType.WATCH_TYPE_PARTITION_CHANGE);
    }

    @Override
    String toNoticeString(WatchResponse res) {
        StringBuilder sb = new StringBuilder();
        return sb.append("graph:").append(res.getPartitionResponse().getGraph())
                .append(",")
                .append("partitionId:").append(res.getPartitionResponse().getPartitionId())
                .toString();
    }

    public void notifyWatcher(WatchChangeType changeType, String graph, int partitionId) {
        isArgumentNotNull(changeType, "changeType");
        isArgumentValid(graph, "graph");

        super.notifyWatcher(builder -> {
            builder.setPartitionResponse(
                    builder.getPartitionResponseBuilder().clear()
                            .setGraph(graph)
                            .setPartitionId(partitionId)
                            .setChangeType(changeType)
                            .build()
            );

        });
    }

}