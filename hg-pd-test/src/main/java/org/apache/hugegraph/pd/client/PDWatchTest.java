package org.apache.hugegraph.pd.client;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.watch.WatchListener;
import org.apache.hugegraph.pd.watch.Watcher;

import org.apache.hugegraph.pd.BaseTest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author lynn.bond@hotmail.com created on 2021/11/8
 */
public class PDWatchTest extends BaseClientTest {

    @BeforeClass
    public static void beforeClass() {
        PDConfig pdConfig = PDConfig.of("localhost:8686,localhost:8687,localhost:8688")
                .setAuthority(BaseTest.user, BaseTest.pwd);
        pdConfig.setEnableCache(true);
        pdClient = PDClient.create(pdConfig);
        try {
            pdClient.getLeader();
        } catch (PDException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void watch() throws IOException {
        Watcher watch = pdClient.getWatchClient();
        CountDownLatch latch = new CountDownLatch(10000);

        Closeable watcher1 = watch.watchNode(new WatchListenerTest(latch, "node-watcher"));
        // PDWatch.Watcher watcher2 = watch.watchPartition(new WatchListener(latch, "watcher2"));
        // PDWatch.Watcher watcher3 = watch.watchPartition(new WatchListener(latch, "watcher3"));

        // PDWatch.Watcher nodeWatcher1 = watch.watchNode(new WatchListener<NodeEvent>(latch, "nodeWatcher1"));

        try {
            latch.await(15000, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        watcher1.close();
        // watcher2.close();
        // watcher3.close();
    }

    private class WatchListenerTest<T> implements WatchListener<T> {

        private final CountDownLatch latch;
        private final String name;

        public WatchListenerTest(CountDownLatch latch, String s) {
            this.latch = latch;
            this.name = s;
        }

        @Override
        public void onNext(T paramT) {

        }

        @Override
        public void onError(Throwable paramThrowable) {
            latch.countDown();
        }
        @Override
        public void onCompleted() {
            latch.countDown();
        }
    }
}