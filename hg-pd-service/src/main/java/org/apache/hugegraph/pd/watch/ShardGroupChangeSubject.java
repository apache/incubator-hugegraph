package com.baidu.hugegraph.pd.watch;

import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.pd.grpc.watch.WatchChangeType;
import com.baidu.hugegraph.pd.grpc.watch.WatchResponse;
import com.baidu.hugegraph.pd.grpc.watch.WatchType;

import static com.baidu.hugegraph.pd.common.HgAssert.isArgumentNotNull;

public class ShardGroupChangeSubject extends AbstractWatchSubject{

    protected ShardGroupChangeSubject() {
        super(WatchType.WATCH_TYPE_SHARD_GROUP_CHANGE);
    }

    @Override
    String toNoticeString(WatchResponse res) {
        StringBuilder sb = new StringBuilder();
        sb.append("shard group:")
                .append(res.getShardGroupResponse().getShardGroup().toString().replace("\n", " "));
        return sb.toString();
    }

    public void notifyWatcher(WatchChangeType changeType, int groupId, Metapb.ShardGroup shardGroup) {
        isArgumentNotNull(changeType, "changeType");

        super.notifyWatcher(builder -> {
            builder.setShardGroupResponse(
                    builder.getShardGroupResponseBuilder().clear()
                            .setShardGroupId(groupId)
                            .setType(changeType)
                            .setShardGroup(shardGroup)
                            .build()
            );
        });
    }
}
