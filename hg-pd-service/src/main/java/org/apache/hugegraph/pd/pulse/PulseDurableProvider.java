package org.apache.hugegraph.pd.pulse;

import org.apache.hugegraph.pd.grpc.Metapb;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Collection;
import java.util.List;

/**
 * @author lynn.bond@hotmail.com on 2023/11/28
 */
@ThreadSafe
public interface PulseDurableProvider {
    boolean isLeader();

    boolean removeQueue(String queueId);

    boolean saveQueue(Metapb.QueueItem queue);

    List<Metapb.QueueItem> queryQueue();

    boolean addNotice(Metapb.NoticeContent notice);

    Metapb.NoticeContent getNotice(long noticeId);

    boolean addObserverNotice(Collection<Metapb.ObserverNotice> observerNotices) ;

    List<Metapb.ObserverNotice> getObserverNotices() ;

    boolean removeObserverNotice(long observerId, long noticeId) ;

    boolean removeNoticeContent(long noticeId) ;

    PulseDurableProvider DEFAULT = new PulseDurableProvider() {
        @Override
        public boolean addNotice(Metapb.NoticeContent notice) {
            return false;
        }

        @Override
        public Metapb.NoticeContent getNotice(long noticeId) {
            return null;
        }

        @Override
        public boolean addObserverNotice(Collection<Metapb.ObserverNotice> observerNotices) {
            return false;
        }

        @Override
        public List<Metapb.ObserverNotice> getObserverNotices() {
            return null;
        }

        @Override
        public boolean removeObserverNotice(long observerId, long noticeId) {
            return false;
        }

        @Override
        public boolean removeNoticeContent(long noticeId) {
            return false;
        }

        @Override
        public boolean isLeader() {
            return false;
        }

        @Override
        public boolean removeQueue(String queueId) {
            return false;
        }

        @Override
        public boolean saveQueue(Metapb.QueueItem queue) {
            return false;
        }

        @Override
        public List<Metapb.QueueItem> queryQueue() {
            return null;
        }

    };
}
