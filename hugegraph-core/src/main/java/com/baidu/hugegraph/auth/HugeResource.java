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

package com.baidu.hugegraph.auth;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonProperty;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonToken;
import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;

import com.baidu.hugegraph.auth.ResourceObject.ResourceType;
import com.baidu.hugegraph.auth.SchemaDefine.UserElement;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.traversal.optimize.TraversalUtil;
import com.baidu.hugegraph.type.Namifiable;
import com.baidu.hugegraph.type.Typifiable;
import com.baidu.hugegraph.util.JsonUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class HugeResource {

    public static final String ANY = "*";

    public static final HugeResource ALL = new HugeResource(ResourceType.ALL,
                                                            ANY, null);
    public static final List<HugeResource> ALL_RES = ImmutableList.of(ALL);

    private static final Set<ResourceType> CHECK_NAME_RESS = ImmutableSet.of(
                                                             ResourceType.META);

    static {
        SimpleModule module = new SimpleModule();

        module.addSerializer(HugeResource.class, new HugeResourceSer());
        module.addDeserializer(HugeResource.class, new HugeResourceDeser());

        JsonUtil.registerModule(module);
    }

    @JsonProperty("type")
    private ResourceType type = ResourceType.NONE;

    @JsonProperty("label")
    private String label = ANY;

    @JsonProperty("properties")
    private Map<String, String> properties; // value can be predicate

    public HugeResource() {
        // pass
    }

    public HugeResource(ResourceType type, String label,
                        Map<String, String> properties) {
        this.type = type;
        this.label = label;
        this.properties = properties;
        this.checkFormat();
    }

    public void checkFormat() {
        if (this.properties == null) {
            return;
        }
        for (Map.Entry<String, String> entry : this.properties.entrySet()) {
            String propName = entry.getKey();
            String propValue = entry.getValue();
            if (propName.equals(ANY) && propValue.equals(ANY)) {
                continue;
            }
            if (propValue.startsWith(TraversalUtil.P_CALL)) {
                TraversalUtil.parsePredicate(propValue);
            }
        }
    }

    public boolean filter(ResourceObject<?> resourceObject) {
        if (this.type == null || this.type == ResourceType.NONE) {
            return false;
        }

        if (!this.type.match(resourceObject.type())) {
            return false;
        }

        if (resourceObject.operated() != NameObject.ANY) {
            ResourceType resType = resourceObject.type();
            if (resType.isGraph()) {
                return this.filter((HugeElement) resourceObject.operated());
            }
            if (resType.isUser()) {
                return this.filter((UserElement) resourceObject.operated());
            }
            if (resType.isSchema() || CHECK_NAME_RESS.contains(resType)) {
                return this.filter((Namifiable) resourceObject.operated());
            }
        }

        /*
         * Allow any others resource if the type is matched:
         * VAR, GREMLIN, GREMLIN_JOB, TASK
         */
        return true;
    }

    private boolean filter(UserElement element) {
        assert this.type.match(element.type());
        if (element instanceof Namifiable) {
            if (!this.filter((Namifiable) element)) {
                return false;
            }
        }
        return true;
    }

    private boolean filter(Namifiable element) {
        assert !(element instanceof Typifiable) || this.type.match(
               ResourceType.from(((Typifiable) element).type()));

        if (!this.matchLabel(element.name())) {
            return false;
        }
        return true;
    }

    private boolean filter(HugeElement element) {
        assert this.type.match(ResourceType.from(element.type()));

        if (!this.matchLabel(element.label())) {
            return false;
        }

        if (this.properties == null) {
            return true;
        }
        for (Map.Entry<String, String> entry : this.properties.entrySet()) {
            String propName = entry.getKey();
            String expected = entry.getValue();
            if (propName.equals(ANY) && expected.equals(ANY)) {
                return true;
            }
            Property<Object> prop = element.property(propName);
            if (!prop.isPresent()) {
                return false;
            }
            Object actual = prop.value();
            if (expected.startsWith(TraversalUtil.P_CALL)) {
                if (!TraversalUtil.parsePredicate(expected).test(actual)) {
                    return false;
                }
            } else {
                if (!expected.equals(actual)) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean matchLabel(String other) {
        // Label value may be vertex/edge label or schema name
        if (this.label == null || other == null) {
            return false;
        }
        if (!this.label.equals(ANY) && !other.matches(this.label)) {
            return false;
        }
        return true;
    }

    private boolean matchProperties(Map<String, String> other) {
        if (this.properties == null) {
            // Any property is OK
            return true;
        }
        if (other == null) {
            return false;
        }
        for (Map.Entry<String, String> p : other.entrySet()) {
            String value = this.properties.get(p.getKey());
            if (!Objects.equals(value, p.getValue())) {
                return false;
            }
        }
        return true;
    }

    protected boolean contains(HugeResource other) {
        if (this.equals(other)) {
            return true;
        }
        if (this.type == null || this.type == ResourceType.NONE) {
            return false;
        }
        if (!this.type.match(other.type)) {
            return false;
        }
        if (!this.matchLabel(other.label)) {
            return false;
        }
        if (!this.matchProperties(other.properties)) {
            return false;
        }
        return true;
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof HugeResource)) {
            return false;
        }
        HugeResource other = (HugeResource) object;
        return this.type == other.type &&
               Objects.equals(this.label, other.label) &&
               Objects.equals(this.properties, other.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.type, this.label, this.properties);
    }

    @Override
    public String toString() {
        return JsonUtil.toJson(this);
    }

    public static boolean allowed(ResourceObject<?> resourceObject) {
        // Allowed to access system(hidden) schema by anyone
        if (resourceObject.type().isSchema()) {
            Namifiable schema = (Namifiable) resourceObject.operated();
            if (Hidden.isHidden(schema.name())) {
                return true;
            }
        }

        return false;
    }

    public static HugeResource parseResource(String resource) {
        return JsonUtil.fromJson(resource, HugeResource.class);
    }

    public static List<HugeResource> parseResources(String resources) {
        TypeReference<?> type = new TypeReference<List<HugeResource>>() {};
        return JsonUtil.fromJson(resources, type);
    }

    public static class NameObject implements Namifiable {

        public static final NameObject ANY = new NameObject("*");

        private final String name;

        public static NameObject of(String name) {
            return new NameObject(name);
        }

        private NameObject(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return this.name;
        }

        @Override
        public String toString() {
            return this.name;
        }
    }

    private static class HugeResourceSer extends StdSerializer<HugeResource> {

        private static final long serialVersionUID = -138482122210181714L;

        public HugeResourceSer() {
            super(HugeResource.class);
        }

        @Override
        public void serialize(HugeResource res, JsonGenerator generator,
                              SerializerProvider provider)
                              throws IOException {
            generator.writeStartObject();

            generator.writeObjectField("type", res.type);
            generator.writeObjectField("label", res.label);
            generator.writeObjectField("properties", res.properties);

            generator.writeEndObject();
        }
    }

    private static class HugeResourceDeser extends StdDeserializer<HugeResource> {

        private static final long serialVersionUID = -2499038590503066483L;

        public HugeResourceDeser() {
            super(HugeResource.class);
        }

        @Override
        public HugeResource deserialize(JsonParser parser,
                                        DeserializationContext ctxt)
                                        throws IOException {
            HugeResource res = new HugeResource();
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String key = parser.getCurrentName();
                if (key.equals("type")) {
                    if (parser.nextToken() != JsonToken.VALUE_NULL) {
                        res.type = ctxt.readValue(parser, ResourceType.class);
                    } else {
                        res.type = null;
                    }
                } else if (key.equals("label")) {
                    if (parser.nextToken() != JsonToken.VALUE_NULL) {
                        res.label = parser.getValueAsString();
                    } else {
                        res.label = null;
                    }
                } else if (key.equals("properties")) {
                    if (parser.nextToken() != JsonToken.VALUE_NULL) {
                        @SuppressWarnings("unchecked")
                        Map<String, String> prop = ctxt.readValue(parser,
                                                                  Map.class);
                        res.properties = prop;
                    } else {
                        res.properties = null;
                    }
                }
            }
            res.checkFormat();
            return res;
        }
    }
}
