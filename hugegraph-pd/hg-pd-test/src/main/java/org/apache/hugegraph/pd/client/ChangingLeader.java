/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.pd.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.pd.common.Useless;

import com.alipay.sofa.jraft.CliService;
import com.alipay.sofa.jraft.RaftServiceFactory;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;

@Useless("used for development")
public class ChangingLeader {

    private static final CliService cliService =
            RaftServiceFactory.createAndInitCliService(new CliOptions());

    public static void main(String[] args) {
        var conf = new Configuration();
        conf.addPeer(PeerId.parsePeer("127.0.0.1:8610"));
        conf.addPeer(PeerId.parsePeer("127.0.0.1:8611"));
        conf.addPeer(PeerId.parsePeer("127.0.0.1:8612"));
        CountDownLatch latch = new CountDownLatch(100);

        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {
            Status status = cliService.transferLeader("pd_raft", conf, PeerId.ANY_PEER);
            System.out.println("trigger change leader status: " + status);
            latch.countDown();
        }, 1, 3, TimeUnit.SECONDS);

        try {
            latch.await();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
