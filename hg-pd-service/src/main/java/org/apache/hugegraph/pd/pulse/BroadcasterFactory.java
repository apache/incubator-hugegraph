package org.apache.hugegraph.pd.pulse;

import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.notice.NoticeBroadcaster;
import org.apache.hugegraph.pd.notice.NoticeDeliver;
import org.apache.hugegraph.pd.util.IdUtil;
import com.google.protobuf.GeneratedMessageV3;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.ThreadSafe;
import java.util.function.Function;

import static org.apache.hugegraph.pd.common.HgAssert.isArgumentNotNull;

/**
 * @author lynn.bond@hotmail.com on 2023/11/7
 */
@Slf4j
@ThreadSafe
final class BroadcasterFactory {
    static PulseDurableProvider durableQueueProvider = PulseDurableProvider.DEFAULT;
    static Function<Class<? extends GeneratedMessageV3>, AbstractObserverSubject> subjectProvider = (e) -> null;

    private BroadcasterFactory() {
    }

    public static NoticeBroadcaster create(GeneratedMessageV3 notice) {
        isArgumentNotNull(notice, "notice");
        AbstractObserverSubject subject = getSubject(notice);

        return NoticeBroadcaster.of(new NoticeDeliverImpl(notice, subject));
    }

    private static <T extends GeneratedMessageV3> AbstractObserverSubject getSubject(T notice) {
        AbstractObserverSubject subject = subjectProvider.apply(notice.getClass());

        if (subject == null) {
            throw new IllegalStateException("Failed to retrieve the subject via notice class: ["
                    + notice.getClass().getTypeName() + "]");
        }

        return subject;
    }

    private static Metapb.QueueItem toQueueItem(GeneratedMessageV3 notice) {
        return Metapb.QueueItem.newBuilder()
                .setItemId(IdUtil.createMillisStr())
                .setItemClass(notice.getClass().getTypeName())
                .setItemContent(notice.toByteString())
                .setTimestamp(System.currentTimeMillis())
                .build();
    }

    /**
     * this inner class is used to deliver notice to client
     */
    private static class NoticeDeliverImpl implements NoticeDeliver {
        private GeneratedMessageV3 notice;
        private AbstractObserverSubject subject;

        public NoticeDeliverImpl(GeneratedMessageV3 notice, AbstractObserverSubject subject) {
            this.notice = notice;
            this.subject = subject;
        }

        @Override
        public boolean isDuty() {
            return durableQueueProvider.isLeader();
        }

        @Override
        public Long send(String durableId) {
            return subject.notifyClient(notice, durableId);
        }

        @Override
        public String save() {
            Metapb.QueueItem queueItem = toQueueItem(notice);
            String res = null;

            try {
                if (durableQueueProvider.saveQueue(queueItem)) {
                    res = queueItem.getItemId();
                } else {
                    log.error("Failed to persist queue item: {}", notice);
                }
            } catch (Throwable t) {
                log.error("Failed to invoke `DurableQueueProvider::saveQueue`, caused by:", t);
            }

            return res;
        }

        @Override
        public boolean remove(String durableId) {
            boolean flag = false;

            try {
                flag = durableQueueProvider.removeQueue(durableId);
            } catch (Throwable t) {
                log.error("Failed to invoke `DurableQueueProvider::removeQueue`, cause by:", t);
            }

            return flag;
        }

        @Override
        public String toNoticeString() {
            return this.notice.toString();
        }
    }

}
