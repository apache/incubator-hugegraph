package com.baidu.hugegraph.vgraph;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.type.define.SerialEnum;

import java.nio.ByteBuffer;

public class VirtualEdge extends VirtualElement {

    private Id ownerVertexId;
    private Directions directions;
    private EdgeLabel edgeLabel;
    private String name;
    private Id otherVertexId;

    public VirtualEdge(HugeEdge edge, byte status) {
        super(edge, status);
        this.ownerVertexId = edge.id().ownerVertexId();
        this.directions = edge.direction();
        this.edgeLabel = edge.schemaLabel();
        this.name = edge.name();
        this.otherVertexId = edge.id().otherVertexId();
    }

    private VirtualEdge() {
        super();
    }

    public Id getOwnerVertexId() {
        return this.ownerVertexId;
    }

    public HugeEdge getEdge(HugeVertex owner) {
        assert owner.id().equals(this.ownerVertexId);
        boolean direction = EdgeId.isOutDirectionFromCode(this.directions.type().code());
        HugeEdge edge = HugeEdge.constructEdge(owner, direction, this.edgeLabel, this.name, this.otherVertexId);
        edge.expiredTime(this.expiredTime);
        this.fillProperties(edge);
        return edge;
    }

    void orStatus(VirtualEdgeStatus status) {
        this.status = status.or(this.status);
    }

    public void writeToBuffer(BytesBuffer buffer) {
        buffer.write(this.status);
        buffer.writeId(this.edgeLabel.id());
        buffer.writeString(this.name);
        buffer.writeId(this.otherVertexId);
        if (this.propertyBuf == null) {
            buffer.writeVInt(0);
        } else {
            buffer.writeBytes(this.propertyBuf.getBytes());
        }
        if (this.edgeLabel.ttl() > 0L) {
            buffer.writeVLong(this.expiredTime);
        }
    }

    public static VirtualEdge readFromBuffer(BytesBuffer buffer, HugeGraph graph, Id ownerVertexId,
                                             Directions directions) {
        VirtualEdge edge = new VirtualEdge();
        edge.ownerVertexId = ownerVertexId;
        edge.status = buffer.read();
        edge.directions = directions;
        edge.edgeLabel = graph.edgeLabelOrNone(buffer.readId());
        edge.name = buffer.readString();
        edge.otherVertexId = buffer.readId();
        int length = buffer.readVInt();
        if (length > 0) {
            BytesBuffer bytesBuffer = BytesBuffer.wrap(buffer.array(), buffer.position(), length).forReadAll();
            edge.propertyBuf = new ByteBufferWrapper(buffer.position(), bytesBuffer.asByteBuffer());
            ByteBuffer innerBuffer = buffer.asByteBuffer();
            innerBuffer.position(bytesBuffer.position());
        }
        if (edge.edgeLabel.ttl() > 0L) {
            edge.expiredTime = buffer.readVLong();
        }
        return edge;
    }
}
