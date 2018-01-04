/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;

import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;

import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.serializer.TextBackendEntry;
import com.baidu.hugegraph.backend.serializer.TextSerializer;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.util.StringEncoding;

public class HugeGraphIoRegistry extends AbstractIoRegistry {

    private static HugeGraphIoRegistry instance;

    public static HugeGraphIoRegistry instance(HugeGraph graph) {
        if (instance == null) {
            synchronized (HugeGraphIoRegistry.class) {
                if (instance == null) {
                    instance = new HugeGraphIoRegistry(graph);
                }
            }
        }
        return instance;
    }

    private static TextSerializer textSerializer = new TextSerializer();

    private HugeGraphIoRegistry(HugeGraph graph) {
        register(GryoIo.class, Optional.class, new OptionalSerializer());
        // HugeGraph related serializer
        register(GryoIo.class, IdGenerator.StringId.class, new IdSerializer());
        register(GryoIo.class, IdGenerator.LongId.class, new IdSerializer());
        register(GryoIo.class, EdgeId.class, new EdgeIdSerializer());

        register(GryoIo.class, PropertyKey.class,
                 new PropertyKeyKryoSerializer());
        register(GryoIo.class, VertexLabel.class,
                 new VertexLabelKryoSerializer());
        register(GryoIo.class, EdgeLabel.class,
                 new EdgeLabelKryoSerializer());
        register(GryoIo.class, IndexLabel.class,
                 new IndexLabelKryoSerializer());

        register(GraphSONIo.class, null, HugeGraphSONModule.instance(graph));
    }

    private class OptionalSerializer extends Serializer<Optional<?>> {
        @Override
        public void write(Kryo kryo, Output output, Optional<?> optional) {
            if (optional.isPresent()) {
                kryo.writeClassAndObject(output, optional.get());
            } else {
                kryo.writeObject(output, null);
            }
        }

        @Override
        public Optional<?> read(Kryo kryo, Input input,
                                Class<Optional<?>> clazz) {
            Object value = kryo.readClassAndObject(input);
            return value == null ? Optional.empty() : Optional.of(value);
        }
    }

    public static class IdSerializer extends Serializer<Id> {

        @Override
        public void write(Kryo kryo, Output output, Id id) {
            output.writeBoolean(id.number());
            byte[] idBytes = id.asBytes();
            output.write(idBytes.length);
            output.writeBytes(id.asBytes());
        }

        @Override
        public Id read(Kryo kryo, Input input, Class<Id> clazz) {
            boolean number = input.readBoolean();
            int length = input.read();
            byte[] idBytes = input.readBytes(length);
            return IdGenerator.of(idBytes, number);
        }
    }

    public static class EdgeIdSerializer extends Serializer<EdgeId> {

        @Override
        public void write(Kryo kryo, Output output, EdgeId edgeId) {
            byte[] idBytes = edgeId.asBytes();
            output.write(idBytes.length);
            output.writeBytes(edgeId.asBytes());
        }

        @Override
        public EdgeId read(Kryo kryo, Input input, Class<EdgeId> aClass) {
            int length = input.read();
            byte[] idBytes = input.readBytes(length);
            return EdgeId.parse(StringEncoding.decode(idBytes));
        }
    }

    private static void writeEntry(Output output, BackendEntry entry) {
        /* Write id */
        byte[] id = entry.id().asBytes();
        output.writeBoolean(entry.id().number());
        output.writeShort(id.length);
        output.writeBytes(id);

        /* Write columns size and data */
        output.writeInt(entry.columns().size());
        for (BackendEntry.BackendColumn c : entry.columns()) {
            output.writeInt(c.name.length);
            output.writeBytes(c.name);
            output.writeInt(c.value.length);
            output.writeBytes(c.value);
        }
    }

    private static BackendEntry readEntry(Input input) {
        /* Read id */
        boolean number = input.readBoolean();
        int idLen = input.readShortUnsigned();
        Id id = IdGenerator.of(input.readBytes(idLen), number);

        /* Read columns size and data */
        Collection<BackendEntry.BackendColumn> columns = new ArrayList<>();
        int columnSize = input.readInt();
        for (int i = 0; i < columnSize; i++) {
            BackendEntry.BackendColumn backendColumn =
                                       new BackendEntry.BackendColumn();
            backendColumn.name = input.readBytes(input.readInt());
            backendColumn.value = input.readBytes(input.readInt());
            columns.add(backendColumn);
        }

        BackendEntry backendEntry = new TextBackendEntry(null, id);
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
        public PropertyKey read(Kryo kryo, Input input,
                                Class<PropertyKey> clazz) {
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
        public VertexLabel read(Kryo kryo,
                                Input input,
                                Class<VertexLabel> clazz) {
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
        public EdgeLabel read(Kryo kryo, Input input, Class<EdgeLabel> clazz) {
            return textSerializer.readEdgeLabel(readEntry(input));
        }
    }

    private class IndexLabelKryoSerializer extends Serializer<IndexLabel> {
        @Override
        public void write(Kryo kryo, Output output, IndexLabel indexLabel) {
            BackendEntry entry = textSerializer.writeIndexLabel(indexLabel);
            writeEntry(output, entry);
        }

        @Override
        public IndexLabel read(Kryo kryo,
                               Input input,
                               Class<IndexLabel> clazz) {
            return textSerializer.readIndexLabel(readEntry(input));
        }
    }
}
