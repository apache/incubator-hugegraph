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

package org.apache.hugegraph.rocksdb.access;

import java.util.Date;
import java.util.HashMap;

import lombok.Data;

/**
 * metadata of a Rocksdb snapshot
 */
@Data
public class DBSnapshotMeta {

    /**
     * graph name
     */
    private String graphName;
    /**
     * partition id
     */
    private int partitionId;
    /**
     * star key with base64 encoding
     */
    private byte[] startKey;
    /**
     * end key with base64 encoding
     */
    private byte[] endKey;
    /**
     * created time of the snapshot
     */
    private Date createdDate;
    /**
     * sst files, key is column family name value is the sst file name of the column
     * family
     */
    private HashMap<String, String> sstFiles;
}
