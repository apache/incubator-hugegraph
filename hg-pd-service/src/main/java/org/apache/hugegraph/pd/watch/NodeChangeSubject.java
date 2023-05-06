package org.apache.hugegraph.pd.watch;

import com.baidu.hugegraph.pd.grpc.watch.NodeEventType;
import com.baidu.hugegraph.pd.grpc.watch.WatchResponse;
import com.baidu.hugegraph.pd.grpc.watch.WatchType;

import javax.annotation.concurrent.ThreadSafe;

import static com.baidu.hugegraph.pd.common.HgAssert.isArgumentNotNull;
import static com.baidu.hugegraph.pd.common.HgAssert.isArgumentValid;

/**
 * The subject of partition change.
 * @author lynn.bond@hotmail.com created on 2021/11/26
 */
@ThreadSafe
final class NodeChangeSubject extends AbstractWatchSubject {

    NodeChangeSubject() {
        super(WatchType.WATCH_TYPE_STORE_NODE_CHANGE);
    }

    @Override
    String toNoticeString(WatchResponse res) {
        StringBuilder sb = new StringBuilder();
        return sb.append("graph:").append(res.getNodeResponse().getGraph())
                .append(",")
                .append("nodeId:").append(res.getNodeResponse().getNodeId())
                .toString();
    }

    public void notifyWatcher(NodeEventType nodeEventType, String graph, long nodeId) {
        isArgumentNotNull(nodeEventType, "nodeEventType");

        super.notifyWatcher(builder -> {
            builder.setNodeResponse(
                    builder.getNodeResponseBuilder().clear()
                            .setGraph(graph)
                            .setNodeId(nodeId)
                            .setNodeEventType(nodeEventType)
                            .build()
            );

        });
    }

    public void notifyError(String message){
        super.notifyError(message);
    }

}