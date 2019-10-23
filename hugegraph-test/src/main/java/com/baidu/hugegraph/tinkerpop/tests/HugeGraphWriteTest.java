/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.tinkerpop.tests;

import java.io.IOException;

import org.apache.tinkerpop.gremlin.process.traversal.IO;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.WriteTest;

import com.baidu.hugegraph.io.HugeGraphIoRegistry;

public class HugeGraphWriteTest extends WriteTest.Traversals {

    @Override
    public Traversal<Object, Object> get_g_io_write_withXwriter_gryoX(
                                     String fileToWrite) throws IOException {
        return g.io(fileToWrite)
                .with(IO.writer, IO.gryo)
                .with(IO.registry, HugeGraphIoRegistry.instance())
                .write();
    }

    @Override
    public Traversal<Object,Object> get_g_io_writeXkryoX(
                                    final String fileToWrite)
                                    throws IOException {
        return g.io(fileToWrite)
                .with(IO.registry, HugeGraphIoRegistry.instance())
                .write();
    }
}
