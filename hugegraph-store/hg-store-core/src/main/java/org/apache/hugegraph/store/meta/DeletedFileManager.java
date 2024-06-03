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

package org.apache.hugegraph.store.meta;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.store.meta.base.GlobalMetaStore;
import org.apache.hugegraph.store.options.MetadataOptions;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeletedFileManager extends GlobalMetaStore {

    public DeletedFileManager(MetadataOptions options) {
        super(options);
    }

    public void load() {
        byte[] key = MetadataKeyHelper.getDeletedFilePrefix();
        List<RocksDBSession.BackendColumn> columns = scan(key);
        for (RocksDBSession.BackendColumn column : columns) {
            String filePath = new String(column.value, StandardCharsets.UTF_8);
            try {
                if (new File(filePath).exists()) {
                    FileUtils.deleteDirectory(new File(filePath));
                    log.warn("Delete legacy files {}", filePath);
                    removeDeletedFile(filePath);
                }
            } catch (IOException e) {
                log.error("Delete legacy files {} exception", filePath, e);
            }
        }
    }

    public void addDeletedFile(String path) {
        byte[] key = MetadataKeyHelper.getDeletedFileKey(path);
        put(key, path.getBytes(StandardCharsets.UTF_8));
    }

    public void removeDeletedFile(String path) {
        byte[] key = MetadataKeyHelper.getDeletedFileKey(path);
        delete(key);
    }

}
