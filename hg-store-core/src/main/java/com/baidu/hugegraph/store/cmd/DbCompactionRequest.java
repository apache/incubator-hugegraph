package com.baidu.hugegraph.store.cmd;

import lombok.Data;

@Data
public class DbCompactionRequest extends HgCmdBase.BaseRequest{
    private String tableName;

    @Override
    public byte magic() {
        return HgCmdBase.ROCKSDB_COMPACTION;
    }
}

