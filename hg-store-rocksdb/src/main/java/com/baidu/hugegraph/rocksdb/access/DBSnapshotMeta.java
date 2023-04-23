package com.baidu.hugegraph.rocksdb.access;

import java.util.Date;
import java.util.HashMap;

import lombok.Data;
import lombok.NonNull;

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
