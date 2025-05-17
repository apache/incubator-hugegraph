package org.apache.hugegraph.pd.pulse;

import org.apache.hugegraph.pd.common.HgAssert;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.notice.NoticeBroadcaster;
import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Parser;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.ThreadSafe;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author lynn.bond@hotmail.com on 2023/11/3
 */
@Slf4j
@ThreadSafe
abstract class RetryingHub {
    private static final int RETRYING_PERIOD_SECONDS = 60;
    private static final int RETRYING_PERIOD_MILLISECONDS = RETRYING_PERIOD_SECONDS * 1000;
    private static final long NOTICE_EXPIRATION_TIME = 30 * 60 * 1000;

    private static final ConcurrentLinkedQueue<NoticeBroadcaster> BROADCASTER_QUEUE = new ConcurrentLinkedQueue<>();
    private static final ScheduledExecutorService SCHEDULED_EXECUTOR = new ScheduledThreadPoolExecutor(1);
    private static final Map<String, Parser> PARSER_HOLDER = new ConcurrentHashMap<>();

    static Function<GeneratedMessageV3, NoticeBroadcaster> broadcasterProvider = (e) -> null;
    static PulseDurableProvider durableQueueProvider = PulseDurableProvider.DEFAULT;

    static {
        /* Initiate retries at the beginning of each minute. e.g. 14:55:00,14:56:00 */
        long currentSeconds = Instant.now().getEpochSecond();
        long nextMinute = (currentSeconds / 60 + 1) * 60;
        long initialDelay = nextMinute - currentSeconds;

        initialDelay += 10; // 10 seconds after the switch starts.

        log.info("Initiate the retries of hub for the pulse notice after [ {} ] seconds.", initialDelay);

        SCHEDULED_EXECUTOR.scheduleAtFixedRate(
                () -> doSchedule(), initialDelay, RETRYING_PERIOD_SECONDS, TimeUnit.SECONDS);
    }

    public static void addQueue(NoticeBroadcaster broadcaster) {
        HgAssert.isArgumentNotNull(broadcaster, "broadcaster");

        log.info("Adding a notice to retrying queue, notice: {}", broadcaster);
        BROADCASTER_QUEUE.add(broadcaster);
    }

    /**
     * Removing a notice from retrying queue via the notice id.
     *
     * @param noticeId
     * @return true if the notice was removed, otherwise false.
     */
    public static boolean removeNotice(long noticeId) {
        return BROADCASTER_QUEUE.removeIf(e -> e.checkAck(noticeId));
    }

    private static void doSchedule() {
        try {
            appendQueue();
            expireQueue();
            BROADCASTER_QUEUE.forEach(e -> {
                retrying(e);
            });
            wipeQueue();
        } catch (Throwable t) {
            log.error("Failed to schedule a notice broadcasting retry, caused by: ", t);
        }
    }

    private static void appendQueue() {
        BROADCASTER_QUEUE.addAll(
                getQueueItems()
                        .parallelStream()
                        .filter(e -> !BROADCASTER_QUEUE
                                .stream()
                                .anyMatch(b -> e.getItemId().equals(b.getDurableId()))
                        ).map(e -> retrieveBroadcaster(e))
                        .peek(e -> log.info("Appending a notice: {}", e))
                        .filter(e -> e != null)
                        .collect(Collectors.toList())
        );
    }

    private static void expireQueue() {
        BROADCASTER_QUEUE.removeIf(e -> {
            if (System.currentTimeMillis() - e.getTimestamp() >= NOTICE_EXPIRATION_TIME) {
                log.info("Notice was expired, trying to remove, notice: {}", e);
                return e.doRemoveDurable();
            } else {
                return false;
            }
        });
    }

    private static void retrying(NoticeBroadcaster broadcaster) {
        if (System.currentTimeMillis() - broadcaster.getTimestamp() < RETRYING_PERIOD_MILLISECONDS) {
            log.info("Skipped the retrying of notice due to 'current-time - creation-time < {} sec'" +
                    ", notice: {}", RETRYING_PERIOD_SECONDS, broadcaster);
            return;
        }

        log.info("Retrying... notice: {}", broadcaster);
        broadcaster.notifying();
    }

    private static void wipeQueue() {
        BROADCASTER_QUEUE.removeIf(e -> {
            if (e.getState() == 10) {
                log.info("Starting to remove an invalid notice from the queue, notice: {}", e);
                return true;
            } else {
                return false;
            }
        });
    }

    private static List<Metapb.QueueItem> getQueueItems() {
        try {
            List<Metapb.QueueItem> items = durableQueueProvider.queryQueue();
            if (items != null) {
                return items;
            }
        } catch (Throwable t) {
            log.error("Failed to retrieve a queue from the DurableQueueProvider, caused by: ", t);
        }

        return Collections.emptyList();
    }

    private static NoticeBroadcaster retrieveBroadcaster(Metapb.QueueItem item) {
        if (item == null) {
            log.error("Failed to create a NoticeBroadcaster, caused by: queue-item is null");
            return null;
        }

        String className = item.getItemClass();
        if (className == null || className.isEmpty()) {
            log.error("Failed to create a NoticeBroadcaster, caused by: class-name is null or empty");
        }

        ByteString instanceData = item.getItemContent();
        if (instanceData == null || instanceData.isEmpty()) {
            log.error("Failed to create a NoticeBroadcaster, caused by: item-content is null or empty");
        }

        GeneratedMessageV3 notice = NoticeParseUtil.parseNotice(instanceData, className);
        if (notice == null) {
            return null;
        }

        NoticeBroadcaster res = toBroadcaster(notice, className);
        if (res == null) {
            return null;
        }

        res.setDurableId(item.getItemId());
        res.setTimestamp(item.getTimestamp());

        return res;
    }

    private static NoticeBroadcaster toBroadcaster(GeneratedMessageV3 notice, String className) {
        NoticeBroadcaster res = null;

        try {
            res = broadcasterProvider.apply(notice);
        } catch (Throwable t) {
            log.error("Failed to fetch a NoticeBroadcaster via Notice instance and Notice class: " + className, t);
        }

        return res;
    }
}
