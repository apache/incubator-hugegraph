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

import org.apache.tinkerpop.shaded.jackson.annotation.JsonProperty;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.JsonMappingException;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;

import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;

public class RolePermission {

    public static final RolePermission NONE = RolePermission.role(
                                              "none", HugePermission.NONE);
    public static final RolePermission ADMIN = RolePermission.role(
                                               "admin", HugePermission.ANY);

    static {
        SimpleModule module = new SimpleModule();

        module.addSerializer(RolePermission.class, new RolePermissionSer());
        module.addDeserializer(RolePermission.class, new RolePermissionDeser());

        JsonUtil.registerModule(module);
    }

    // Mapping of: graph -> action -> resource
    @JsonProperty("roles")
    private final Map<String, Map<HugePermission, List<HugeResource>>> roles;

    public RolePermission() {
        this(new TreeMap<>());
    }

    private RolePermission(Map<String, Map<HugePermission,
                                       List<HugeResource>>> roles) {
        this.roles = roles;
    }

    protected void add(String graph, String action,
                       List<HugeResource> resources) {
        this.add(graph, HugePermission.valueOf(action), resources);
    }

    protected void add(String graph, HugePermission action,
                       List<HugeResource> resources) {
        Map<HugePermission, List<HugeResource>> permissions =
                                                this.roles.get(graph);
        if (permissions == null) {
            permissions = new TreeMap<>();
            this.roles.put(graph, permissions);
        }
        List<HugeResource> mergedResources = permissions.get(action);
        if (mergedResources == null) {
            mergedResources = new ArrayList<>();
            permissions.put(action, mergedResources);
        }
        mergedResources.addAll(resources);
    }

    protected Map<String, Map<HugePermission, List<HugeResource>>> map() {
        return Collections.unmodifiableMap(this.roles);
    }

    protected boolean contains(RolePermission other) {
        for (Map.Entry<String, Map<HugePermission, List<HugeResource>>> e1 :
             other.roles.entrySet()) {
            String g = e1.getKey();
            Map<HugePermission, List<HugeResource>> perms = this.roles.get(g);
            if (perms == null) {
                return false;
            }
            for (Map.Entry<HugePermission, List<HugeResource>> e2 :
                e1.getValue().entrySet()) {
                List<HugeResource> ress = perms.get(e2.getKey());
                if (ress == null) {
                    return false;
                }
                for (HugeResource r : e2.getValue()) {
                    boolean contains = false;
                    for (HugeResource res : ress) {
                        if (res.contains(r)) {
                            contains = true;
                            break;
                        }
                    }
                    if (!contains) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof RolePermission)) {
            return false;
        }
        RolePermission other = (RolePermission) object;
        return Objects.equals(this.roles, other.roles);
    }

    @Override
    public String toString() {
        return this.roles.toString();
    }

    public String toJson() {
        return JsonUtil.toJson(this);
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
        return role(graph, HugePermission.ANY);
    }

    public static RolePermission role(String graph, HugePermission perm) {
        RolePermission role = new RolePermission();
        role.add(graph, perm, HugeResource.ALL_RES);
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

    private static class RolePermissionSer
                   extends StdSerializer<RolePermission> {

        private static final long serialVersionUID = -2533310506459479383L;

        public RolePermissionSer() {
            super(RolePermission.class);
        }

        @Override
        public void serialize(RolePermission role, JsonGenerator generator,
                              SerializerProvider provider)
                              throws IOException {
            generator.writeStartObject();
            generator.writeObjectField("roles", role.roles);
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
            if ("roles".equals(parser.nextFieldName())) {
                parser.nextValue();
                return new RolePermission(parser.readValueAs(type));
            }
            throw JsonMappingException.from(parser, "Expect field roles");
        }
    }
}
