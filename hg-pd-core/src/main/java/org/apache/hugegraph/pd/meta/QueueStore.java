package org.apache.hugegraph.pd.meta;

import com.baidu.hugegraph.pd.common.HgAssert;
import com.baidu.hugegraph.pd.common.PDException;

import org.apache.hugegraph.pd.config.PDConfig;

import com.baidu.hugegraph.pd.grpc.Metapb;

import java.util.List;

/**
 * @author lynn.bond@hotmail.com on 2022/2/10
 */
public class QueueStore extends MetadataRocksDBStore {
    QueueStore(PDConfig pdConfig) {
        super(pdConfig);
    }

    public void addItem(Metapb.QueueItem queueItem) throws PDException {
        HgAssert.isArgumentNotNull(queueItem, "queueItem");
        byte[] key = MetadataKeyHelper.getQueueItemKey(queueItem.getItemId());
        put(key, queueItem.toByteString().toByteArray());
    }

    public void removeItem(String itemId) throws PDException {
        remove(MetadataKeyHelper.getQueueItemKey(itemId));
    }

    public List<Metapb.QueueItem> getQueue() throws PDException {
        byte[] prefix = MetadataKeyHelper.getQueueItemPrefix();
        return scanPrefix(Metapb.QueueItem.parser(), prefix);
    }
}
