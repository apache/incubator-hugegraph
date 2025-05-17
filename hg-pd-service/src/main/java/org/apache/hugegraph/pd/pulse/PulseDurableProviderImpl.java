package org.apache.hugegraph.pd.pulse;

import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.meta.MetadataFactory;

import org.apache.hugegraph.pd.meta.PulseStore;
import org.apache.hugegraph.pd.raft.RaftEngine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author lynn.bond@hotmail.com on 2023/11/28
 */
@Slf4j
@Component("durableQueueProvider")
public class PulseDurableProviderImpl implements PulseDurableProvider {
    @Autowired
    private PDConfig pdConfig;
    private PulseStore pulseStore = null;

    @Override
    public boolean removeQueue(String queueId) {
        if (queueId == null) {
            return false;
        }

        try {
            this.getPulseStore().removeItem(queueId);
            return true;
        } catch (Throwable t) {
            log.error("Failed to remove item from store, item-id: " + queueId + ", caused by:", t);
        }

        return false;
    }

    @Override
    public boolean saveQueue(Metapb.QueueItem queue) {
        if (queue == null) {
            return false;
        }

        try {
            this.getPulseStore().addItem(queue);
            return true;
        } catch (Throwable t) {
            log.error("Failed to add item to store, item: " + queue.toString() + ", caused by:", t);
        }

        return false;
    }

    @Override
    public List<Metapb.QueueItem> queryQueue() {
        if (!isLeader()) {
            return Collections.emptyList();
        }

        try {
            return this.getPulseStore().getQueue();
        } catch (Throwable t) {
            log.error("Failed to retrieve queue from PulseStore, caused by:", t);
        }

        log.warn("Returned empty queue list.");
        return Collections.emptyList();
    }

    @Override
    public boolean addNotice(Metapb.NoticeContent notice) {
        try {
            this.getPulseStore().addNotice(notice);
            return true;
        } catch (Throwable t) {
            log.error("Failed to add notice to store, notice: " + notice.toString() + ", caused by:", t);
        }

        return false;
    }

    @Override
    public Metapb.NoticeContent getNotice(long noticeId) {
        try {
            return this.getPulseStore().getNotice(noticeId);
        } catch (Throwable t) {
            log.error("Failed to retrieve notice from PulseStore, caused by:", t);
        }

        return null;
    }

    @Override
    public boolean addObserverNotice(Collection<Metapb.ObserverNotice> observerNotices) {
        // TODO: implement a batch add method
        try {
            for (Metapb.ObserverNotice observerNotice : observerNotices) {
                this.getPulseStore().addObserverNotice(observerNotice);
            }
            return true;
        } catch (Throwable t) {
            log.error("Failed to add a collection of observer notices to the store, observerNotices: "
                    + observerNotices.toString() + ", caused by:", t);
        }

        return false;
    }

    @Override
    public List<Metapb.ObserverNotice> getObserverNotices() {
        try {
            return this.getPulseStore().getObserverNotices();
        } catch (Throwable t) {
            log.error("Failed to retrieve observer notices from PulseStore, caused by:", t);
        }

        return Collections.emptyList();
    }

    @Override
    public boolean removeObserverNotice(long observerId, long noticeId) {
        try {
            this.getPulseStore().removeObserverNotice(observerId, noticeId);
            return true;
        } catch (Throwable t) {
            log.error("Failed to remove observer notice from store, "
                    + "observerId: " + observerId + ", noticeId: " + noticeId
                    + ", caused by:", t);
        }

        return false;
    }

    @Override
    public boolean removeNoticeContent(long noticeId) {
        try {
            this.getPulseStore().removeNoticeContent(noticeId);
            return true;
        }catch (Throwable t) {
            log.error("Failed to remove notice content from store, "
                    + "noticeId: " + noticeId
                    + ", caused by:", t);
        }
        return false;
    }

    @Override
    public boolean isLeader() {
        return RaftEngine.getInstance().isLeader();
    }

    private PulseStore getPulseStore() {
        if (this.pulseStore == null) {
            synchronized (this) {
                if (this.pulseStore == null) {
                    this.pulseStore = MetadataFactory.newPulseStore(this.pdConfig);
                }
            }
        }

        return this.pulseStore;
    }
}
