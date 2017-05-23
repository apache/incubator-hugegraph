package com.baidu.hugegraph.io;

import java.util.Collection;
import java.util.LinkedList;

import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.id.IdGeneratorFactory;
import com.baidu.hugegraph.backend.serializer.TextBackendEntry;
import com.baidu.hugegraph.backend.serializer.TextSerializer;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.schema.HugeEdgeLabel;
import com.baidu.hugegraph.schema.HugePropertyKey;
import com.baidu.hugegraph.schema.HugeVertexLabel;
import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;

public class HugeGraphIoRegistry extends AbstractIoRegistry {

    private static HugeGraphIoRegistry INSTANCE = new HugeGraphIoRegistry();

    public static HugeGraphIoRegistry getInstance() {
        return INSTANCE;
    }

    private static TextSerializer textSerializer = new TextSerializer(null);

    public HugeGraphIoRegistry() {
        register(GryoIo.class, IdGenerator.StringId.class, new IdSerializer());
        register(GryoIo.class, HugePropertyKey.class, new PropertyKeyKryoSerializer());
        register(GryoIo.class, HugeVertexLabel.class, new VertexLabelKryoSerializer());
        register(GryoIo.class, HugeEdgeLabel.class, new EdgeLabelKryoSerializer());

        register(GraphSONIo.class, null, HugeGraphSONModule.getInstance());
    }

    public static class IdSerializer extends Serializer<Id> {
        @Override
        public void write(Kryo kryo, Output output, Id id) {
            output.writeString(id.asString());
        }

        @Override
        public Id read(Kryo kryo, Input input, Class<Id> aClass) {
            return IdGeneratorFactory.generator().generate(input.readString());
        }
    }

    private static void writeEntry(Output output, BackendEntry entry) {
        // write id
        output.writeInt(entry.id().asBytes().length);
        output.writeBytes(entry.id().asBytes());

        // write columns size and data
        output.writeInt(entry.columns().size());
        for (BackendEntry.BackendColumn c : entry.columns()) {
            output.writeInt(c.name.length);
            output.writeBytes(c.name);
            output.writeInt(c.value.length);
            output.writeBytes(c.value);
        }
    }

    private static BackendEntry readEntry(Input input) {
        // read id
        int idLen = input.readInt();
        Id id = IdGeneratorFactory.generator().parse(input.readBytes(idLen));

        // read columns size and data
        Collection<BackendEntry.BackendColumn> columns = new LinkedList<>();
        int columnSize = input.readInt();
        for (int i = 0; i < columnSize; i++) {
            BackendEntry.BackendColumn backendColumn = new BackendEntry.BackendColumn();
            backendColumn.name = input.readBytes(input.readInt());
            backendColumn.value = input.readBytes(input.readInt());
            columns.add(backendColumn);
        }
        BackendEntry backendEntry = new TextBackendEntry(id);
        backendEntry.columns(columns);
        return backendEntry;
    }

    private class PropertyKeyKryoSerializer extends Serializer<PropertyKey> {
        @Override
        public void write(Kryo kryo, Output output, PropertyKey propertyKey) {
            BackendEntry entry = textSerializer.writePropertyKey(propertyKey);
            writeEntry(output, entry);
        }

        @Override
        public PropertyKey read(Kryo kryo, Input input, Class<PropertyKey> aClass) {
            return textSerializer.readPropertyKey(readEntry(input));
        }
    }

    private class VertexLabelKryoSerializer extends Serializer<VertexLabel> {
        @Override
        public void write(Kryo kryo, Output output, VertexLabel vertexLabel) {
            BackendEntry entry = textSerializer.writeVertexLabel(vertexLabel);
            writeEntry(output, entry);
        }

        @Override
        public VertexLabel read(Kryo kryo, Input input, Class<VertexLabel> aClass) {
            return textSerializer.readVertexLabel(readEntry(input));
        }
    }

    private class EdgeLabelKryoSerializer extends Serializer<EdgeLabel> {

        @Override
        public void write(Kryo kryo, Output output, EdgeLabel edgeLabel) {
            BackendEntry entry = textSerializer.writeEdgeLabel(edgeLabel);
            writeEntry(output, entry);
        }

        @Override
        public EdgeLabel read(Kryo kryo, Input input, Class<EdgeLabel> aClass) {
            return textSerializer.readEdgeLabel(readEntry(input));
        }
    }


}
