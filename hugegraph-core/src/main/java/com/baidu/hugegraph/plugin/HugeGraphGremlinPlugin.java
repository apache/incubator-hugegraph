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

package com.baidu.hugegraph.plugin;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;
import org.apache.tinkerpop.gremlin.tinkergraph.jsr223.TinkerGraphGremlinPlugin;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.util.ReflectionUtil;
import com.google.common.reflect.ClassPath;

public class HugeGraphGremlinPlugin extends AbstractGremlinPlugin {

    private static final String PACKAGE = "com.baidu.hugegraph";
    private static final String NAME = "com.baidu.hugegraph";

    private static final ImportCustomizer imports;

    static {
        Iterator<ClassPath.ClassInfo> classInfos;
        try {
            classInfos = ReflectionUtil.classes(PACKAGE);
        } catch (IOException e) {
            throw new HugeException("Failed to scan classes under package %s",
                                    PACKAGE);
        }

        Set<Class> classes = new HashSet<>();
        classInfos.forEachRemaining(classInfo -> classes.add(classInfo.load()));
        imports = DefaultImportCustomizer.build()
                                         .addClassImports(classes)
                                         .create();
    }

    private static final HugeGraphGremlinPlugin instance =
            new HugeGraphGremlinPlugin();

    public HugeGraphGremlinPlugin() {
        super(NAME, imports);
    }

    public static HugeGraphGremlinPlugin instance() {
        return instance;
    }
}
