package org.apache.hugegraph.pd.meta;

import org.apache.hugegraph.pd.common.HgAssert;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;

import java.util.List;

/**
 * @author lynn.bond@hotmail.com on 2022/2/10
 */
public class PulseStore extends MetadataRocksDBStore {
    PulseStore(PDConfig pdConfig) {
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

    /*****************************************************************
     *  The following methods are for the retying notice dispatcher  *
     ****************************************************************/

    public void addNotice(Metapb.NoticeContent noticeContent) throws PDException {
        HgAssert.isArgumentNotNull(noticeContent, "noticeContent");
        byte[] key = MetadataKeyHelper.getNoticeContentKey(noticeContent.getNoticeId());
        put(key, noticeContent.toByteString().toByteArray());
    }

    public Metapb.NoticeContent getNotice(long noticeId) throws PDException {
        byte[] key = MetadataKeyHelper.getNoticeContentKey(noticeId);
        return getOne(Metapb.NoticeContent.parser(), key);
    }

    public void addObserverNotice(Metapb.ObserverNotice observerNotice) throws PDException {
        HgAssert.isArgumentNotNull(observerNotice, "observerNotice");
        byte[] key = MetadataKeyHelper.getObserverNoticeKey(observerNotice.getObserverId(),
                observerNotice.getNoticeId());
        put(key, observerNotice.toByteString().toByteArray());
    }

    public List<Metapb.ObserverNotice> getObserverNotices() throws PDException {
        byte[] prefix = MetadataKeyHelper.getObserverNoticePrefix();
        return scanPrefix(Metapb.ObserverNotice.parser(), prefix);
    }

    public void removeObserverNotice(long observerId, long noticeId) throws PDException {
        remove(MetadataKeyHelper.getObserverNoticeKey(observerId, noticeId));
    }

    public void removeNoticeContent(long noticeId) throws PDException {
        remove(MetadataKeyHelper.getNoticeContentKey(noticeId));
    }

}
