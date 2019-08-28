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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.util.E;

public class HugeUser {

    /*
     * TODO: add vertex label: group and action
     * add edge label belongto: user belongto group
     * add edge label allowed: group allowed action
     * action: write/read vertex|edge(limit label), write/read schema
     */

    private final Id id;
    private String name;
    private String password;
    private String phone;
    private String email;
    private String avatar;
    private Date create;
    private Date update;

    public HugeUser(String name) {
        this(null, name);
    }

    public HugeUser(Id id) {
        this(id, null);
    }

    public HugeUser(Id id, String name) {
        this.id = id;
        this.name = name;
        this.create = new Date();
        this.update = this.create;
    }

    public Id id() {
        return this.id;
    }

    public String name() {
        return this.name;
    }

    public String password() {
        return this.password;
    }

    public void password(String password) {
        this.password = password;
    }

    public String phone() {
        return this.phone;
    }

    public void phone(String phone) {
        this.phone = phone;
    }

    public String email() {
        return this.email;
    }

    public void email(String email) {
        this.email = email;
    }

    public String avatar() {
        return this.avatar;
    }

    public void avatar(String avatar) {
        this.avatar = avatar;
    }

    public Date create() {
        return this.create;
    }

    public void create(Date create) {
        this.create = create;
    }

    public Date update() {
        return this.update;
    }

    public void update(Date update) {
        this.update = update;
    }

    @Override
    public String toString() {
        return String.format("HugeUser(%s)%s", this.id, this.asMap());
    }

    protected void property(String key, Object value) {
        E.checkNotNull(key, "property key");
        switch (key) {
            case P.NAME:
                this.name = (String) value;
                break;
            case P.PASSWORD:
                this.password = (String) value;
                break;
            case P.PHONE:
                this.phone = (String) value;
                break;
            case P.EMAIL:
                this.email = (String) value;
                break;
            case P.AVATAR:
                this.avatar = (String) value;
                break;
            case P.CREATE:
                this.create = (Date) value;
                break;
            case P.UPDATE:
                this.update = (Date) value;
                break;
            default:
                throw new AssertionError("Unsupported key: " + key);
        }
    }

    protected Object[] asArray() {
        E.checkState(this.name != null, "User name can't be null");
        E.checkState(this.password != null, "User password can't be null");
        E.checkState(this.create != null, "User create can't be null");
        E.checkState(this.update != null, "User update can't be null");

        List<Object> list = new ArrayList<>(16);

        list.add(T.label);
        list.add(P.USER);

        list.add(P.NAME);
        list.add(this.name);

        list.add(P.PASSWORD);
        list.add(this.password);

        if (this.phone != null) {
            list.add(P.PHONE);
            list.add(this.phone);
        }

        if (this.email != null) {
            list.add(P.EMAIL);
            list.add(this.email);
        }

        if (this.avatar != null) {
            list.add(P.AVATAR);
            list.add(this.avatar);
        }

        list.add(P.CREATE);
        list.add(this.create);

        list.add(P.UPDATE);
        list.add(this.update);

        return list.toArray();
    }

    public Map<String, Object> asMap() {
        E.checkState(this.name != null, "User name can't be null");
        E.checkState(this.password != null, "User password can't be null");
        E.checkState(this.create != null, "User create can't be null");
        E.checkState(this.update != null, "User update can't be null");

        Map<String, Object> map = new HashMap<>();

        if (this.id != null) {
            // The id is null when creating user
            map.put(Hidden.unHide(P.ID), this.id);
        }

        map.put(Hidden.unHide(P.NAME), this.name);
        map.put(Hidden.unHide(P.PASSWORD), this.password);

        if (this.phone != null) {
            map.put(Hidden.unHide(P.PHONE), this.phone);
        }

        if (this.email != null) {
            map.put(Hidden.unHide(P.EMAIL), this.email);
        }

        if (this.avatar != null) {
            map.put(Hidden.unHide(P.AVATAR), this.avatar);
        }

        map.put(Hidden.unHide(P.CREATE), this.create);
        map.put(Hidden.unHide(P.UPDATE), this.update);

        return map;
    }

    public static <V> HugeUser fromVertex(Vertex vertex) {
        HugeUser task = new HugeUser((Id) vertex.id());
        for (Iterator<VertexProperty<Object>> iter = vertex.properties();
             iter.hasNext();) {
            VertexProperty<Object> prop = iter.next();
            task.property(prop.key(), prop.value());
        }
        return task;
    }

    public static final class P {

        public static final String USER = Hidden.hide("user");

        public static final String ID = T.id.getAccessor();
        public static final String LABEL = T.label.getAccessor();

        public static final String NAME = "~user_name";
        public static final String PASSWORD = "~user_password";
        public static final String PHONE = "~user_phone";
        public static final String EMAIL = "~user_email";
        public static final String AVATAR = "~user_avatar";
        public static final String CREATE = "~user_create";
        public static final String UPDATE = "~user_update";

        public static String unhide(String key) {
            final String prefix = Hidden.hide("user_");
            if (key.startsWith(prefix)) {
                return key.substring(prefix.length());
            }
            return key;
        }
    }
}
