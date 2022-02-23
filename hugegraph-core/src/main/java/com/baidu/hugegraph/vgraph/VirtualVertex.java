package com.baidu.hugegraph.vgraph;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.baidu.hugegraph.iterator.ExtendableIterator;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.define.Directions;

public class VirtualVertex extends VirtualElement {

    private Id id;
    private VertexLabel label;
    private BytesBuffer outEdgesBuf;
    private BytesBuffer inEdgesBuf;

    public VirtualVertex(HugeVertex vertex, byte status) {
        super(vertex, status);
        assert vertex != null;
        this.id = vertex.id();
        this.label = vertex.schemaLabel();
        this.outEdgesBuf = null;
        this.inEdgesBuf = null;
    }

    public HugeVertex getVertex(HugeGraph graph) {
        HugeVertex vertex = new HugeVertex(graph, id, label);
        vertex.expiredTime(this.expiredTime);
        this.fillProperties(vertex);
        return vertex;
    }

    public Iterator<VirtualEdge> getEdges(HugeGraph graph) {
        ExtendableIterator<VirtualEdge> result = new ExtendableIterator<>();
        List<VirtualEdge> outEdges = readEdgesFromBuffer(this.outEdgesBuf, graph, Directions.OUT);
        if (outEdges != null) {
            result.extend(outEdges.listIterator());
        }
        List<VirtualEdge> inEdges = readEdgesFromBuffer(this.inEdgesBuf, graph, Directions.IN);
        if (inEdges != null) {
            result.extend(inEdges.listIterator());
        }
        return result;
    }

    public void addOutEdges(List<VirtualEdge> edges) {
        this.outEdgesBuf = writeEdgesToBuffer(edges);
    }

    public void addInEdges(List<VirtualEdge> edges) {
        this.inEdgesBuf = writeEdgesToBuffer(edges);
    }

    public void copyInEdges(VirtualVertex other) {
        this.inEdgesBuf = other.inEdgesBuf;
    }

    private BytesBuffer writeEdgesToBuffer(List<VirtualEdge> edges) {
        assert edges != null;
        if (edges.size() > 0) {
            BytesBuffer buffer = new BytesBuffer();
            // Write edge list size
            buffer.writeVInt(edges.size());
            // Write edge
            for (VirtualEdge edge: edges) {
                edge.writeToBuffer(buffer);
            }
            return buffer;
        } else {
            return null;
        }
    }

    private List<VirtualEdge> readEdgesFromBuffer(BytesBuffer buffer, HugeGraph graph, Directions directions) {
        if (buffer != null) {
            ByteBuffer byteBuffer = buffer.asByteBuffer();
            BytesBuffer wrapedBuffer = BytesBuffer.wrap(byteBuffer.array(),
                    byteBuffer.arrayOffset(), byteBuffer.position());

            int size = wrapedBuffer.readVInt();
            assert size >= 0;
            List<VirtualEdge> result = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                VirtualEdge edge = VirtualEdge.readFromBuffer(wrapedBuffer, graph, this.id, directions);
                result.add(edge);
            }
            return result;
        } else {
            return null;
        }
    }

    void orStatus(VirtualVertexStatus status) {
        this.status = status.or(this.status);
    }
}
