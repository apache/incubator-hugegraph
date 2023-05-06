package org.apache.hugegraph.pd.client;

import org.apache.hugegraph.pd.client.test.HgPDTestUtil;
import org.apache.hugegraph.pd.watch.NodeEvent;

import org.junit.BeforeClass;
// import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author lynn.bond@hotmail.com created on 2021/11/8
 */
@Deprecated
public class PDWatchTest {
    private static PDClient pdClient;

    private long storeId = 0;
    private String storeAddr = "localhost";
    private String graphName = "graph1";

    @BeforeClass
    public static void beforeClass() throws Exception {
        pdClient = PDClient.create(PDConfig.of("localhost:9000"));
    }

    // @Test
    public void watch(){
        PDWatch watch=pdClient.getWatchClient();
        CountDownLatch latch = new CountDownLatch(10);

        PDWatch.Watcher watcher1=watch.watchPartition(new WatchListener(latch,"watcher1"));
        PDWatch.Watcher watcher2=watch.watchPartition(new WatchListener(latch,"watcher2"));
        PDWatch.Watcher watcher3=watch.watchPartition(new WatchListener(latch,"watcher3"));

        PDWatch.Watcher nodeWatcher1=watch.watchNode(new WatchListener<NodeEvent>(latch,"nodeWatcher1"));

        try {
            latch.await(15, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        watcher1.close();
        watcher2.close();
        watcher3.close();
    }

    private class WatchListener<T> implements PDWatch.Listener<T>{
        CountDownLatch latch = new CountDownLatch(10);
        private String watcherName;

        private WatchListener(CountDownLatch latch,String watcherName){
            this.latch=latch;
            this.watcherName=watcherName;
        }

        @Override
        public void onNext(T response) {
            HgPDTestUtil.println(this.watcherName + " res: " + response);
            this.latch.countDown();
        }

        @Override
        public void onError(Throwable throwable) {
            HgPDTestUtil.println(this.watcherName + " error: " + throwable.toString());
        }

        @Override
        public void onCompleted() {
            HgPDTestUtil.println(this.watcherName + " is completed");
        }
    }
}