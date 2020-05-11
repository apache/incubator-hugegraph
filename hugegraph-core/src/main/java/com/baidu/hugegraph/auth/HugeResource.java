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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonProperty;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonToken;
import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.JsonMappingException;
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
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;
import com.google.common.collect.ImmutableList;

public class HugeResource {

    public static final String ANY = "*";

    public static final HugeResource ALL = new HugeResource(ResourceType.ALL,
                                                            ANY, null);
    public static final List<HugeResource> ALL_RES = ImmutableList.of(ALL);

    static {
        SimpleModule module = new SimpleModule();

        module.addSerializer(HugeResource.class, new HugeResourceSer());
        module.addDeserializer(HugeResource.class, new HugeResourceDeser());

        module.addSerializer(RolePermission.class, new RolePermissionSer());
        module.addDeserializer(RolePermission.class, new RolePermissionDeser());

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
    }

    public boolean filter(ResourceObject<?> resourceObject) {
        if (this.type == null || this.type == ResourceType.NONE) {
            return false;
        }

        if (!this.type.match(resourceObject.type())) {
            return false;
        }

        if (resourceObject.type().isGraph()) {
            return this.filter((HugeElement) resourceObject.operated());
        }
        if (resourceObject.type().isSchema()) {
            return this.filter((Namifiable) resourceObject.operated());
        }
        if (resourceObject.type().isUser()) {
            return this.filter((UserElement) resourceObject.operated());
        }

        /*
         * Allow any others resource if the type is matched:
         * VAR, GREMLIN, GREMLIN_JOB, TASK
         */
        return true;
    }

    private boolean filter(UserElement element) {
        assert this.type.match(element.type());
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

    private boolean matchLabel(String value) {
        if (this.label == null || value == null) {
            return false;
        }
        if (!this.label.equals(ANY) && !value.matches(this.label)) {
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

    public static HugeResource parseResource(String resource) {
        return JsonUtil.fromJson(resource, HugeResource.class);
    }

    public static List<HugeResource> parseResources(String resources) {
        TypeReference<?> type = new TypeReference<List<HugeResource>>() {};
        return JsonUtil.fromJson(resources, type);
    }

    public static class RolePermission {

        public static final RolePermission NONE = RolePermission.role(
                                                  "none", HugePermission.NONE);
        public static final RolePermission ADMIN = RolePermission.role(
                                                   "admin", HugePermission.ALL);

        // Mapping of: graph -> action -> resource
        @JsonProperty("roles")
        private final Map<String, Map<HugePermission, List<HugeResource>>> map;

        public RolePermission() {
            this(new TreeMap<>());
        }

        private RolePermission(Map<String, Map<HugePermission,
                                               List<HugeResource>>> map) {
            this.map = map;
        }

        protected void add(String graph, String action,
                           List<HugeResource> resources) {
            this.add(graph, HugePermission.valueOf(action), resources);
        }

        protected void add(String graph, HugePermission action,
                           List<HugeResource> resources) {
            Map<HugePermission, List<HugeResource>> permissions;
            permissions = this.map.get(graph);
            if (permissions == null) {
                permissions = new TreeMap<>();
                this.map.put(graph, permissions);
            }
            List<HugeResource> mergedResources = permissions.get(action);
            if (mergedResources == null) {
                mergedResources = new ArrayList<>();
                permissions.put(action, mergedResources);
            }
            mergedResources.addAll(resources);
        }

        protected Map<String, Map<HugePermission, List<HugeResource>>> map() {
            return Collections.unmodifiableMap(this.map);
        }

        @Override
        public boolean equals(Object object) {
            if (!(object instanceof RolePermission)) {
                return false;
            }
            RolePermission other = (RolePermission) object;
            return Objects.equals(this.map, other.map);
        }

        @Override
        public String toString() {
            return this.map.toString();
        }

        public static RolePermission fromJson(Object json) {
            RolePermission role;
            if (json instanceof String) {
                role = JsonUtil.fromJson((String) json, RolePermission.class);
            } else {
                // Optimized json with RolePermission object
                E.checkArgument(json instanceof RolePermission,
                                "Invalid role value: %s", json);
                role = (RolePermission) json;
            }
            return role;
        }

        public static RolePermission all(String graph) {
            return role(graph, HugePermission.ALL);
        }

        public static RolePermission role(String graph, HugePermission perm) {
            RolePermission role = new RolePermission();
            role.add(graph, perm, ALL_RES);
            return role;
        }

        public static RolePermission none() {
            return NONE;
        }

        public static RolePermission admin() {
            return ADMIN;
        }

        public static RolePermission builtin(RolePermission role) {
            E.checkNotNull(role, "role");
            if (role == ADMIN || role.equals(ADMIN)) {
                return ADMIN;
            }
            if (role == NONE || role.equals(NONE)) {
                return NONE;
            }
            return role;
        }
    }

    public static class NameObject implements Namifiable {

        private final String name;

        public NameObject(String name) {
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
            return res;
        }
    }

    private static class RolePermissionSer extends StdSerializer<RolePermission> {

        private static final long serialVersionUID = -2533310506459479383L;

        public RolePermissionSer() {
            super(RolePermission.class);
        }

        @Override
        public void serialize(RolePermission role, JsonGenerator generator,
                              SerializerProvider provider)
                              throws IOException {
            generator.writeStartObject();
            generator.writeObjectField("roles", role.map);
            generator.writeEndObject();
        }
    }

    private static class RolePermissionDeser extends StdDeserializer<RolePermission> {

        private static final long serialVersionUID = -2038234657843260957L;

        public RolePermissionDeser() {
            super(RolePermission.class);
        }

        @Override
        public RolePermission deserialize(JsonParser parser,
                                          DeserializationContext ctxt)
                                          throws IOException {
            TypeReference<?> type = new TypeReference<TreeMap<String,
                             TreeMap<HugePermission, List<HugeResource>>>>() {};
            if (parser.nextFieldName().equals("roles")) {
                parser.nextValue();
                return new RolePermission(parser.readValueAs(type));
            }
            throw JsonMappingException.from(parser, "Expect field roles");
        }
    }
}
