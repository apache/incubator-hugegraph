package com.baidu.hugegraph.io;

import java.util.Iterator;
import java.util.List;

import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.id.IdGeneratorFactory;
import com.baidu.hugegraph.schema.HugeEdgeLabel;
import com.baidu.hugegraph.schema.HugePropertyKey;
import com.baidu.hugegraph.schema.HugeVertexLabel;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.util.JsonUtil;

public class HugeGraphIoRegistry extends AbstractIoRegistry {

    private static HugeGraphIoRegistry INSTANCE = new HugeGraphIoRegistry();

    public static HugeGraphIoRegistry getInstance() {
        return INSTANCE;
    }

    public HugeGraphIoRegistry() {
        register(GryoIo.class, IdGenerator.StringId.class, new IdSerializer());
//        register(GryoIo.class, HugePropertyKey.class, new HugePropertyKeySerializer());
//        register(GryoIo.class, HugeVertexLabel.class, new HugeVertexLabelSerializer());
//        register(GryoIo.class, HugeEdgeLabel.class, new HugeEdgeLabelSerializer());
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

//    private class HugePropertyKeySerializer extends Serializer<HugePropertyKey> {
//        @Override
//        public void write(Kryo kryo, Output output, HugePropertyKey propertyKey) {
//            kryo.writeObject(output, propertyKey);
//        }
//
//        @Override
//        public HugePropertyKey read(Kryo kryo, Input input, Class<HugePropertyKey> aClass) {
//            return kryo.readObject(input, HugePropertyKey.class);
//        }
//    }
//
//    private class HugeVertexLabelSerializer extends Serializer<HugeVertexLabel> {
//        @Override
//        public void write(Kryo kryo, Output output, HugeVertexLabel vertexLabel) {
//            kryo.writeObject(output, vertexLabel);
//        }
//
//        @Override
//        public HugeVertexLabel read(Kryo kryo, Input input, Class<HugeVertexLabel> aClass) {
//            return kryo.readObject(input, HugeVertexLabel.class);
//        }
//    }
//
//    private class HugeEdgeLabelSerializer extends Serializer<HugeEdgeLabel> {
//
//        @Override
//        public void write(Kryo kryo, Output output, HugeEdgeLabel edgeLabel) {
//            kryo.writeObject(output, edgeLabel);
//        }
//
//        @Override
//        public HugeEdgeLabel read(Kryo kryo, Input input, Class<HugeEdgeLabel> aClass) {
//            return kryo.readObject(input, HugeEdgeLabel.class);
//        }
//    }
}
