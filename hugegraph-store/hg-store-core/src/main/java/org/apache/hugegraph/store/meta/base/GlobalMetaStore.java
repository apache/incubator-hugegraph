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

package org.apache.hugegraph.store.meta.base;

import java.util.Arrays;

import org.apache.hugegraph.rocksdb.access.RocksDBFactory;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.store.options.MetadataOptions;

public class GlobalMetaStore extends MetaStoreBase {

    public static final String HSTORE_METADATA_GRAPH_NAME = "hgstore-metadata";
    public static final String HSTORE_CF_NAME = "default";

    private final MetadataOptions options;

    private final String dataPath;

    public GlobalMetaStore(MetadataOptions options) {
        this.options = options;
        dataPath = Arrays.asList(options.getDataPath().split(",")).get(0);
    }

    public MetadataOptions getOptions() {
        return options;
    }

    @Override
    protected RocksDBSession getRocksDBSession() {
        RocksDBFactory rocksDBFactory = RocksDBFactory.getInstance();
        RocksDBSession dbSession = rocksDBFactory.queryGraphDB(HSTORE_METADATA_GRAPH_NAME);
        if (dbSession == null) {
            dbSession = rocksDBFactory.createGraphDB(dataPath, HSTORE_METADATA_GRAPH_NAME);
        }
        return dbSession;
    }

    @Override
    protected String getCFName() {
        return HSTORE_CF_NAME;
    }
}
