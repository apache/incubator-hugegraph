package com.baidu.hugegraph.io;

import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.id.IdGeneratorFactory;

public class HugeGraphIoRegistry extends AbstractIoRegistry {

    private static HugeGraphIoRegistry INSTANCE = new HugeGraphIoRegistry();

    public static HugeGraphIoRegistry getInstance() {
        return INSTANCE;
    }

    public HugeGraphIoRegistry() {
        register(GryoIo.class, IdGenerator.StringId.class, new IdSerializer());
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
}
