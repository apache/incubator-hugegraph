package org.apache.hugegraph.pd.client;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.pulse.PartitionHeartbeatRequest;
import org.apache.hugegraph.pd.pulse.Pulse;
import org.apache.hugegraph.pd.pulse.PulseListener;
import org.apache.hugegraph.pd.pulse.PulseNotifier;
import org.apache.hugegraph.pd.pulse.PulseServerNotice;
import org.apache.hugegraph.pd.test.HgPDTestUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author lynn.bond@hotmail.com created on 2021/11/8
 */
public class PDPulseTest extends BaseClientTest {

    @BeforeClass
    public static void beforeClass() {
        PDConfig pdConfig = PDConfig.of("localhost:8686"
                        + ",localhost:8687,localhost:8688"
                )
                .setAuthority(user, pwd);
        pdConfig.setEnableCache(true);
        pdClient = PDClient.create(pdConfig);
        try {
            pdClient.getLeader();
        } catch (PDException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void listen() {
        listenWithObserverId(0L);
    }

    @Test
    public void listenWith1() {
        this.listenWithObserverId(1L);
    }

    @Test
    public void listenWith2() {
        this.listenWithObserverId(2L);
    }

    @Test
    public void listenWith3() {
        this.listenWithObserverId(3L);
    }

    public void forceReconnect() {
//        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {
//            pdClient.forceReconnect();
//        }, 1, 2, TimeUnit.SECONDS);
    }

    private void listenWithObserverId(long observerId) {
        Pulse pulse;
        if (observerId > 0L) {
            pulse = pdClient.getPulse(observerId);
        } else {
            pulse = pdClient.getPulse();
        }

        CountDownLatch latch = new CountDownLatch(100000);

        PulseNotifier<PartitionHeartbeatRequest.Builder> notifier1 = pulse.connect(new PulseListenerTest(latch, "test-listener"));

        try {
            latch.await(3600 * 24, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            notifier1.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //@Test
    public void notifyServer() {
        CountDownLatch latch = new CountDownLatch(100);
        Pulse pulse = pdClient.getPulse();
        PulseNotifier<PartitionHeartbeatRequest.Builder> notifier =
                pulse.connect(new PulseListenerTest<>(latch, "test-listener"));
        for (int i = 0; i < 100; i++) {
            HgPDTestUtil.println("Notifying server [" + i + "] times.");
            notifier.notifyServer(PartitionHeartbeatRequest.newBuilder().setStates(
                    Metapb.PartitionStats.newBuilder().setId(i)
            ));
        }

    }

    private class PulseListenerTest<T> implements PulseListener<T> {
        CountDownLatch latch = new CountDownLatch(10);
        private String listenerName;

        private PulseListenerTest(CountDownLatch latch, String listenerName) {
            this.latch = latch;
            this.listenerName = listenerName;
        }

        @Override
        public void onNext(T response) {
            // println(this.listenerName+" res: "+response);
            // this.latch.countDown();
        }

        @Override
        public void onNotice(PulseServerNotice<T> notice) {
            try {
                log("Sleeping for ACK, noticeID: " + notice.getNoticeId());
                Thread.sleep(100);
                //  TimeUnit.SECONDS.sleep(15);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            notice.ack();
            this.latch.countDown();
        }

        @Override
        public void onError(Throwable throwable) {
            HgPDTestUtil.println(this.listenerName + " error: " + throwable.toString());
        }

        @Override
        public void onCompleted() {
            HgPDTestUtil.println(this.listenerName + " is completed");
        }


    }

    public static void log(String s) {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        StringBuffer sb = new StringBuffer();
        sb.append(now.format(formatter)).append(" ").append(s);

        System.out.println(sb);
    }
}