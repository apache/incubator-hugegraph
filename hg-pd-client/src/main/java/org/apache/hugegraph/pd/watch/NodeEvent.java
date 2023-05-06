package org.apache.hugegraph.pd.watch;

import com.baidu.hugegraph.pd.grpc.watch.NodeEventType;

import java.util.Objects;

/**
 * @author lynn.bond@hotmail.com created on 2021/11/4
 */
public class NodeEvent {
    private String graph;
    private long nodeId;
    private EventType eventType;

    public NodeEvent(String graph, long nodeId, EventType eventType) {
        this.graph=graph;
        this.nodeId=nodeId;
        this.eventType=eventType;
    }

    public String getGraph() {
        return graph;
    }

    public long getNodeId() {
        return nodeId;
    }

    public EventType getEventType() {
        return eventType;
    }

    public enum EventType {
        UNKNOWN,
        NODE_ONLINE,
        NODE_OFFLINE,
        NODE_RAFT_CHANGE;


        public static EventType grpcTypeOf(NodeEventType grpcType) {
            switch (grpcType) {
                case NODE_EVENT_TYPE_NODE_ONLINE:
                    return NODE_ONLINE;
                case NODE_EVENT_TYPE_NODE_OFFLINE:
                    return NODE_OFFLINE;
                case NODE_EVENT_TYPE_NODE_RAFT_CHANGE:
                    return NODE_RAFT_CHANGE;
                default:
                    return UNKNOWN;
            }

        }

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeEvent nodeEvent = (NodeEvent) o;
        return nodeId == nodeEvent.nodeId && Objects.equals(graph,
                nodeEvent.graph) && eventType == nodeEvent.eventType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(graph, nodeId, eventType);
    }

    @Override
    public String toString() {
        return "NodeEvent{" +
                "graph='" + graph + '\'' +
                ", nodeId=" + nodeId +
                ", eventType=" + eventType +
                '}';
    }
}
