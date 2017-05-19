package com.baidu.hugegraph.io;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.io.graphson.TinkerPopJacksonModule;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;

import com.baidu.hugegraph.backend.serializer.TextBackendEntry;
import com.baidu.hugegraph.backend.serializer.TextSerializer;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;

/**
 * Created by liningrui on 2017/5/18.
 */
public class HugeGraphSONModule extends TinkerPopJacksonModule {

    private static final String TYPE_NAMESPACE = "hugegraph";

    private static TextSerializer textSerializer = new TextSerializer(null);

    private static final Map<Class, String> TYPE_DEFINITIONS = Collections.unmodifiableMap(
            new LinkedHashMap<Class, String>() {{
                put(PropertyKey.class, "PropertyKey");
                put(VertexLabel.class, "VertexLabel");
                put(EdgeLabel.class, "EdgeLabel");
            }});

    private static final HugeGraphSONModule INSTANCE = new HugeGraphSONModule();

    public static final HugeGraphSONModule getInstance() {
        return INSTANCE;
    }

    public HugeGraphSONModule() {
        super(TYPE_NAMESPACE);

        addSerializer(PropertyKey.class, new PropertyKeySerializer());
        addDeserializer(PropertyKey.class, new PropertyKeyDeserializer());
        addSerializer(VertexLabel.class, new VertexLabelSerializer());
        addDeserializer(VertexLabel.class, new VertexLabelDeserializer());
        addSerializer(EdgeLabel.class, new EdgeLabelSerializer());
        addDeserializer(EdgeLabel.class, new EdgeLabelDeserializer());
    }

    @Override
    public Map<Class, String> getTypeDefinitions() {
        return TYPE_DEFINITIONS;
    }

    @Override
    public String getTypeNamespace() {
        return TYPE_NAMESPACE;
    }

    private static void writeEntry(JsonGenerator jsonGenerator, TextBackendEntry entry) throws IOException {
        jsonGenerator.writeStartObject();
        // write id
        jsonGenerator.writeStringField(HugeKeys.ID.string(), entry.id().asString());

        // write columns size and data
        for (String name : entry.columnNames()) {
            jsonGenerator.writeFieldName(name);
            jsonGenerator.writeRawValue(entry.column(name));
        }
        jsonGenerator.writeEndObject();
    }

    private class PropertyKeySerializer extends StdSerializer<PropertyKey> {

        public PropertyKeySerializer() {
            super(PropertyKey.class);
        }

        @Override
        public void serialize(PropertyKey propertyKey, JsonGenerator jsonGenerator,
                              SerializerProvider serializerProvider)
                throws IOException {
            BackendEntry entry = textSerializer.writePropertyKey(propertyKey);
            writeEntry(jsonGenerator, (TextBackendEntry) entry);
        }
    }

    private class PropertyKeyDeserializer extends StdDeserializer<PropertyKey> {

        public PropertyKeyDeserializer() {
            super(PropertyKey.class);
        }

        @Override
        public PropertyKey deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException, JsonProcessingException {
            return null;
        }
    }

    private class VertexLabelSerializer extends StdSerializer<VertexLabel> {

        public VertexLabelSerializer() {
            super(VertexLabel.class);
        }

        @Override
        public void serialize(VertexLabel vertexLabel, JsonGenerator jsonGenerator,
                              SerializerProvider serializerProvider)
                throws IOException {
            BackendEntry entry = textSerializer.writeVertexLabel(vertexLabel);
            writeEntry(jsonGenerator, (TextBackendEntry) entry);
        }

    }

    private class VertexLabelDeserializer extends StdDeserializer<VertexLabel> {

        public VertexLabelDeserializer() {
            super(VertexLabel.class);
        }

        @Override
        public VertexLabel deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException, JsonProcessingException {

            return null;
        }

    }

    private class EdgeLabelSerializer extends StdSerializer<EdgeLabel> {

        public EdgeLabelSerializer() {
            super(EdgeLabel.class);
        }

        @Override
        public void serialize(EdgeLabel edgeLabel, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
                throws IOException {
            BackendEntry entry = textSerializer.writeEdgeLabel(edgeLabel);
            writeEntry(jsonGenerator, (TextBackendEntry) entry);
        }
    }

    private class EdgeLabelDeserializer extends StdDeserializer<EdgeLabel> {

        public EdgeLabelDeserializer() {
            super(EdgeLabel.class);
        }

        @Override
        public EdgeLabel deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException, JsonProcessingException {
            return null;
        }
    }
}
