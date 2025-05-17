package org.apache.hugegraph.pd.client;

import com.alipay.sofa.jraft.CliService;
import com.alipay.sofa.jraft.RaftServiceFactory;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ChangingLeader {

    private static CliService cliService = RaftServiceFactory.createAndInitCliService(new CliOptions());

    public static void main(String[] args) {
        var conf = new Configuration();
        conf.addPeer(PeerId.parsePeer("127.0.0.1:8610"));
        conf.addPeer(PeerId.parsePeer("127.0.0.1:8611"));
        conf.addPeer(PeerId.parsePeer("127.0.0.1:8612"));
        CountDownLatch latch = new CountDownLatch(100);

        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {
            Status status = cliService.transferLeader("pd_raft", conf, PeerId.ANY_PEER);
            System.out.println("trigger change leader status: "+status);
            latch.countDown();
        }, 1,3, TimeUnit.SECONDS);

        try {
            latch.await();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
