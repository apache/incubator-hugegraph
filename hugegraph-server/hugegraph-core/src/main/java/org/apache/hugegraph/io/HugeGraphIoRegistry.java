/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.io;

import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.Io;

public class HugeGraphIoRegistry extends AbstractIoRegistry {

    private static final HugeGraphIoRegistry INSTANCE =
                         new HugeGraphIoRegistry();

    public static HugeGraphIoRegistry instance() {
        return INSTANCE;
    }

    private HugeGraphIoRegistry() {
        HugeGryoModule.register(this);
        HugeGraphSONModule.register(this);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void register(Class<? extends Io> ioClass, Class clazz, Object ser) {
        super.register(ioClass, clazz, ser);
    }
}
