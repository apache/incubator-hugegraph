package org.apache.hugegraph.pd.meta;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;

import java.util.List;

/**
 * @author zhangyingjie
 * @date 2022/3/29
 **/
public class LogMeta extends MetadataRocksDBStore {

    private PDConfig pdConfig;

    public LogMeta(PDConfig pdConfig) {
        super(pdConfig);
        this.pdConfig = pdConfig;
    }

    public void insertLog(Metapb.LogRecord record) throws PDException {
        byte[] storeLogKey = MetadataKeyHelper.getLogKey(record);
        put(storeLogKey, record.toByteArray());

    }

    public List<Metapb.LogRecord> getLog(String action, Long start, Long end) throws PDException {
        byte[] keyStart = MetadataKeyHelper.getLogKeyPrefix(action, start);
        byte[] keyEnd = MetadataKeyHelper.getLogKeyPrefix(action, end);
        List<Metapb.LogRecord> stores =this.scanRange(Metapb.LogRecord.parser(),
                                                      keyStart, keyEnd);
        return stores;
    }
}
