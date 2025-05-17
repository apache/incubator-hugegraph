package org.apache.hugegraph.pd.pulse;

import org.apache.hugegraph.pd.common.HgAssert;
import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.util.IdUtil;
import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.ThreadSafe;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author lynn.bond@hotmail.com on 2024/2/21
 */
@Slf4j
@ThreadSafe
public class RetryingSwitch {
    private static final int RETRYING_PERIOD_SECONDS = 60;
    private static final int RETRYING_PERIOD_MILLISECONDS = RETRYING_PERIOD_SECONDS * 1000;
    private static final long NOTICE_EXPIRATION_TIME = 1000 * 60 * 30; /** 30 minutes */

    private static final ScheduledExecutorService SCHEDULED_EXECUTOR = new ScheduledThreadPoolExecutor(1);
    private static final ConcurrentHashMap<Long, KVPair<GeneratedMessageV3, Long>> NOTICE_CACHE = new ConcurrentHashMap<>();
    private static final Queue<Metapb.ObserverNotice> EXPIRED_BUFFER = new ConcurrentLinkedQueue<>();

    static PulseDurableProvider pulseDurableProvider = PulseDurableProvider.DEFAULT;
    static Function<Class<? extends GeneratedMessageV3>, AbstractObserverSubject> subjectProvider = (e) -> null;

    static {
        /* Initiate retries at the beginning of each minute. e.g. 14:55:00,14:56:00 */
        long currentSeconds = Instant.now().getEpochSecond();
        long nextMinute = (currentSeconds / 60 + 1) * 60;
        long initialDelay = nextMinute - currentSeconds;

        log.info("Initiate the retries of switch for the pulse notice after [ {} ] seconds.", initialDelay);
        SCHEDULED_EXECUTOR.scheduleAtFixedRate(
                () -> doSchedule(), initialDelay, RETRYING_PERIOD_SECONDS, TimeUnit.SECONDS);

    }

    /**
     * @param notice
     * @param observerIds
     * @throws IllegalArgumentException if notice or observerIds is null
     */
    public static boolean addNotice(GeneratedMessageV3 notice, Collection<Long> observerIds) {
        HgAssert.isArgumentNotNull(notice, "notice");
        HgAssert.isFalse(HgAssert.isInvalid(observerIds), "The argument is invalid: observerIds");

        Metapb.NoticeContent noticeContent = toNoticeContent(notice);

        if (!saveNotice(noticeContent, observerIds)) {
            return false;
        }

        return sending(notice, noticeContent.getNoticeId(), observerIds);
    }

    public static boolean ackNotice(long noticeId, long observerId) {
        log.info("Ack remove notice: [ {} ], observer: [ {} ] ", noticeId, observerId);
        return pulseDurableProvider.removeObserverNotice(observerId, noticeId);
    }

    private static boolean saveNotice(Metapb.NoticeContent noticeContent, Collection<Long> observerIds) {

        if (!pulseDurableProvider.addNotice(noticeContent)) {
            log.error("Failed to add notice: {}", noticeContent);
            return false;
        }

        if (!pulseDurableProvider.addObserverNotice(toObserverNotices(noticeContent.getNoticeId(), observerIds))) {
            log.error("Failed to add observer notice: {}", noticeContent);
            return false;
        }

        return true;
    }

    private static void doSchedule() {
        try {
            if (isOnDuty()) {
                retrying();
                wipeExpired();
                cleanCache();
            } else {
                log.debug("Not on duty, skip retrying.");
            }
        } catch (Throwable t) {
            log.error("Failed to schedule a notice broadcasting retry, caused by: ", t);
        }
    }

    private static boolean isOnDuty() {
        return pulseDurableProvider.isLeader();
    }

    private static void retrying() {
        List<Metapb.ObserverNotice> observerNotices = pulseDurableProvider.getObserverNotices();
        Map<Long, List<Long>> noticeObservers = toNoticeObservers(observerNotices);

        log.info("Retrying notices of switch, amount: {}", noticeObservers.size());

        noticeObservers.forEach(
                (noticeId, observerIds) -> {
                    sending(noticeId, observerIds);
                });
    }

    private static boolean sending(long noticeId, Collection<Long> observerIds) {
        GeneratedMessageV3 notice = getNotice(noticeId);

        if (notice == null) {
            log.error("Failed to get notice for id: {}", noticeId);
            return false;
        }

        return sending(notice, noticeId, observerIds);
    }

    private static boolean sending(GeneratedMessageV3 notice, long noticeId, Collection<Long> observerIds) {
        AbstractObserverSubject subject = subjectProvider.apply(notice.getClass());

        if (subject == null) {
            log.error("Failed to get an observer subject for notice: {}", notice);
            return false;
        }

        subject.notifyClient(notice, noticeId, observerIds);

        return true;
    }

    private static void wipeExpired() {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        while (!EXPIRED_BUFFER.isEmpty()) {
            Metapb.ObserverNotice notice = EXPIRED_BUFFER.poll();
            Date createdTime = new Date(notice.getTimestamp());
            log.info("Removing expired notice: [ {} ], observer: [ {} ], created: [ {} ]",
                    notice.getNoticeId(), notice.getObserverId(), formatter.format(createdTime));
            NOTICE_CACHE.remove(notice.getNoticeId());
            pulseDurableProvider.removeObserverNotice(notice.getObserverId(), notice.getNoticeId());
            pulseDurableProvider.removeNoticeContent(notice.getNoticeId());
        }
    }

    /**
     * Remove expired notice from NOTICE_CACHE and remove it from store.
     */
    private static void cleanCache() {
        Iterator<Map.Entry<Long, KVPair<GeneratedMessageV3, Long>>> iterator = NOTICE_CACHE.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<Long, KVPair<GeneratedMessageV3, Long>> entry = iterator.next();
            long noticeId = entry.getKey();
            long intoCacheTime = entry.getValue().getValue();

            if (System.currentTimeMillis() - intoCacheTime >= NOTICE_EXPIRATION_TIME) {
                log.info("Cleaning a cached notice: [ {} ]", noticeId);
                iterator.remove();
                pulseDurableProvider.removeNoticeContent(noticeId);
            }

        }
    }

    private static GeneratedMessageV3 getNotice(long noticeId) {
        KVPair<GeneratedMessageV3, Long> tsNotice = NOTICE_CACHE.get(noticeId);
        if (tsNotice != null) {
            return tsNotice.getKey();
        }

        Metapb.NoticeContent noticeContent = pulseDurableProvider.getNotice(noticeId);
        if (noticeContent == null) {
            log.warn("Failed to get notice content for id: {}", noticeId);
            return null;
        }

        String className = noticeContent.getNoticeClass();
        if (className == null || className.isEmpty()) {
            log.error("Failed to get notice class for id: {}", noticeId);
            return null;
        }

        ByteString instanceData = noticeContent.getNoticeContent();
        if (instanceData == null || instanceData.isEmpty()) {
            log.error("Failed to create a notice, caused by: notice-content is null or empty");
            return null;
        }

        GeneratedMessageV3 notice = NoticeParseUtil.parseNotice(instanceData, className);

        if (notice == null) {
            log.error("Failed to parse a notice, caused by: parse notice content failed.");
            return null;
        }

        // KVPair<notice, into_cache_time>
        NOTICE_CACHE.put(noticeId, new KVPair<>(notice, System.currentTimeMillis()));

        return notice;
    }

    private static Map<Long, List<Long>> toNoticeObservers(List<Metapb.ObserverNotice> notices) {
        return notices.stream()
                .filter(RetryingSwitch::isNotExpired)
                .filter(RetryingSwitch::isNotBrandNew)
                .collect(
                        Collectors.groupingBy(Metapb.ObserverNotice::getNoticeId,
                                Collectors.mapping(Metapb.ObserverNotice::getObserverId, Collectors.toList()))
                );
    }

    private static boolean isNotExpired(Metapb.ObserverNotice notice) {
        if (System.currentTimeMillis() - notice.getTimestamp() < NOTICE_EXPIRATION_TIME) {
            return true;
        } else {
            EXPIRED_BUFFER.offer(notice);
            return false;
        }
    }

    private static boolean isNotBrandNew(Metapb.ObserverNotice notice) {
        if (System.currentTimeMillis() - notice.getTimestamp() > RETRYING_PERIOD_MILLISECONDS) {
            return true;
        } else {
            return false;
        }
    }

    private static Metapb.NoticeContent toNoticeContent(GeneratedMessageV3 notice) {
        return Metapb.NoticeContent.newBuilder()
                .setNoticeId(IdUtil.createMillisId())
                .setNoticeClass(notice.getClass().getName())
                .setNoticeContent(notice.toByteString())
                .setTimestamp(System.currentTimeMillis())
                .build();
    }

    private static Collection<Metapb.ObserverNotice> toObserverNotices(long noticeId, Collection<
            Long> observerIds) {
        long timestamp = System.currentTimeMillis();
        return observerIds.stream()
                .map(e -> Metapb.ObserverNotice.newBuilder()
                        .setObserverId(e)
                        .setNoticeId(noticeId)
                        .setTimestamp(timestamp)
                        .build()
                ).collect(Collectors.toList());
    }

}
