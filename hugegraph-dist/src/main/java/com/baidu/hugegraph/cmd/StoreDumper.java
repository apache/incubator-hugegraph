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

package com.baidu.hugegraph.cmd;

import java.util.Iterator;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.dist.RegisterUtil;
import com.baidu.hugegraph.event.EventHub;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;

public class StoreDumper {

    private final HugeGraph graph;

    public StoreDumper(String conf) {
        this.graph = HugeFactory.open(conf);
    }

    public void dump(HugeType table, long offset, long limit) {
        BackendStore store = table.isGraph() ?
                             this.graph.graphTransaction().store() :
                             this.graph.schemaTransaction().store();

        Query query = new Query(table);
        Iterator<BackendEntry> rs = store.query(query);
        for (long i = 0; i < offset && rs.hasNext(); i++) {
            rs.next();
        }
        String title = String.format("Dump table %s (offset %d limit %d):",
                                     table, offset, limit);
        System.out.println(title);
        for (long i = 0; i < limit && rs.hasNext(); i++) {
            BackendEntry entry = rs.next();
            System.out.println(entry);
        }

        CloseableIterator.closeIterator(rs);
    }

    public void close() {
        this.graph.close();
    }

    public static void main(String[] args)
                       throws ConfigurationException, InterruptedException {
        E.checkArgument(args.length >= 1,
                        "StoreDumper need a config file.");

        String conf = args[0];
        RegisterUtil.registerBackends();

        HugeType table = HugeType.valueOf(arg(args, 1, "VERTEX").toUpperCase());
        long offset = Long.valueOf(arg(args, 2, "0"));
        long limit = Long.valueOf(arg(args, 3, "20"));

        StoreDumper dumper = new StoreDumper(conf);
        dumper.dump(table, offset, limit);
        dumper.close();

        // Wait cache clear or init up to 30 seconds
        EventHub.destroy(30);
    }

    private static String arg(String[] args, int index, String deflt) {
        if (index < args.length) {
            return args[index];
        }
        return deflt;
    }
}
