package com.baidu.hugegraph.store.cmd;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class BatchPutRequest extends HgCmdBase.BaseRequest{
    private List<KV> entries;
    @Override
    public byte magic() {
        return HgCmdBase.BATCH_PUT;
    }

    @Data
    public static class KV implements Serializable {
        private String table;
        private int code;
        private byte[] key;
        private byte[] value;

        public static KV of(String table, int code, byte[] key, byte[] value){
            KV kv = new KV();
            kv.table = table;
            kv.code = code;
            kv.key = key;
            kv.value = value;
            return kv;
        }
    }
}
