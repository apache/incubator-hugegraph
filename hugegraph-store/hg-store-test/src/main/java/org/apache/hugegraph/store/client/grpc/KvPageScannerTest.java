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

package org.apache.hugegraph.store.client.grpc;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.hugegraph.store.client.HgStoreNode;
import org.apache.hugegraph.store.client.HgStoreNodeManager;
import org.apache.hugegraph.store.client.HgStoreNodeSession;
import org.apache.hugegraph.store.client.HgStoreNotice;
import org.apache.hugegraph.store.client.type.HgNodeStatus;
import org.apache.hugegraph.store.client.type.HgStoreClientException;
import org.apache.hugegraph.store.grpc.common.Kv;
import org.apache.hugegraph.store.grpc.common.ScanMethod;
import org.apache.hugegraph.store.grpc.common.ScanOrderType;
import org.apache.hugegraph.store.grpc.stream.KvPageRes;
import org.apache.hugegraph.store.grpc.stream.ScanStreamReq;
import org.junit.Assert;
import org.junit.Test;

import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

public class KvPageScannerTest {

    private static final AtomicLong NODE_IDS = new AtomicLong(-9000000);

    @Test
    public void testUnavailableScanRetiresPoolAndRebindsNextStub() {
        try (FailureFixture fixture = new FailureFixture()) {
            var oldStub = fixture.client.getStub(fixture.session);
            Assert.assertTrue(Arrays.asList(fixture.channels).contains(oldStub.getChannel()));
            fixture.failScan(Status.UNAVAILABLE);

            Assert.assertNull(fixture.manager.getStoreNode(fixture.nodeId));
            Assert.assertFalse(AbstractGrpcClient.channels.containsKey(fixture.target));
            for (ManagedChannel channel : fixture.channels) {
                verify(channel).shutdown();
            }
            var nextStub = fixture.client.getStub(fixture.session);
            Assert.assertFalse(Arrays.asList(fixture.channels).contains(nextStub.getChannel()));
        }
    }

    @Test
    public void testCancelledAndInvalidScansPreserveHealthyPool() {
        for (Status status : List.of(Status.CANCELLED, Status.INVALID_ARGUMENT)) {
            try (FailureFixture fixture = new FailureFixture()) {
                fixture.failScan(status);
                Assert.assertSame(fixture.node, fixture.manager.getStoreNode(fixture.nodeId));
                Assert.assertSame(fixture.channels, AbstractGrpcClient.channels.get(fixture.target));
                for (ManagedChannel channel : fixture.channels) {
                    verify(channel, never()).shutdown();
                }
            }
        }
    }

    @Test
    public void testLateScanFailurePreservesSameAddressReplacement() {
        try (FailureFixture fixture = new FailureFixture()) {
            HgStoreNode replacement = mock(HgStoreNode.class);
            when(replacement.getNodeId()).thenReturn(fixture.nodeId);
            when(replacement.getAddress()).thenReturn(fixture.target);
            fixture.manager.addNode(fixture.graph, replacement);
            fixture.failScan(Status.UNAVAILABLE);

            Assert.assertSame(replacement, fixture.manager.getStoreNode(fixture.nodeId));
            Assert.assertSame(fixture.channels, AbstractGrpcClient.channels.get(fixture.target));
            for (ManagedChannel channel : fixture.channels) {
                verify(channel, never()).shutdown();
            }
            fixture.manager.notifying(fixture.graph,
                                      HgStoreNotice.of(fixture.nodeId, HgNodeStatus.NOT_WORK),
                                      replacement);
        }
    }

    private static class FailureFixture implements AutoCloseable {

        private final long nodeId = NODE_IDS.decrementAndGet();
        private final String graph = "scan-recovery-" + this.nodeId;
        private final String target = this.graph + ":8500";
        private final HgStoreNodeManager manager = HgStoreNodeManager.getInstance();
        private final HgStoreNode node = mock(HgStoreNode.class);
        private final HgStoreNodeSession session = mock(HgStoreNodeSession.class);
        private final ManagedChannel[] channels = new ManagedChannel[AbstractGrpcClient.concurrency];
        private final GrpcStoreStreamClient client = new GrpcStoreStreamClient() {
            @Override
            protected ManagedChannel createChannel(String target) {
                return mock(ManagedChannel.class);
            }
        };

        FailureFixture() {
            when(this.node.getNodeId()).thenReturn(this.nodeId);
            when(this.node.getAddress()).thenReturn(this.target);
            when(this.session.getStoreNode()).thenReturn(this.node);
            when(this.session.getGraphName()).thenReturn(this.graph);
            this.manager.addNode(this.graph, this.node);
            Arrays.setAll(this.channels, ignored -> mock(ManagedChannel.class));
            AbstractGrpcClient.channels.put(this.target, this.channels);
        }

        void failScan(Status status) {
            var error = status.withDescription("scan transport failure").asRuntimeException();
            KvPageScanner scanner = new KvPageScanner(this.session,
                    ScanStreamReq.newBuilder().setMethod(ScanMethod.ALL).setTable("table"),
                    response -> new StreamObserver<ScanStreamReq>() {
                        @Override
                        public void onNext(ScanStreamReq request) {
                            response.onError(error);
                        }

                        @Override
                        public void onError(Throwable t) {
                        }

                        @Override
                        public void onCompleted() {
                        }
                    });
            try {
                scanner.hasNext();
                Assert.fail("Failed scan must remain visible to the caller");
            } catch (HgStoreClientException e) {
                Assert.assertSame(error, e.getCause());
            }
        }

        @Override
        public void close() {
            this.manager.notifying(this.graph,
                                   HgStoreNotice.of(this.nodeId, HgNodeStatus.NOT_WORK), this.node);
            AbstractGrpcClient.closeChannel(this.target);
        }
    }

    @Test
    public void testKeepsExplicitPageSizeAndRequestsNextPageOnDemand() {
        List<ScanStreamReq> requests = new ArrayList<>();
        Function<StreamObserver<KvPageRes>, StreamObserver<ScanStreamReq>>
                streamFactory = response -> requestObserver(requests, response);
        ScanStreamReq.Builder builder = ScanStreamReq.newBuilder()
                                                     .setMethod(ScanMethod.RANGE)
                                                     .setTable("table")
                                                     .setPageSize(2);
        KvPageScanner scanner = new KvPageScanner(session(), builder,
                                                  streamFactory);

        Assert.assertEquals(0, requests.size());
        Assert.assertTrue(scanner.hasNext());
        Assert.assertEquals(1, requests.size());
        Assert.assertEquals(2, requests.get(0).getPageSize());

        Assert.assertEquals(1, scanner.next().getKey().byteAt(0) & 0xff);
        Assert.assertTrue(scanner.hasNext());
        Assert.assertEquals(1, requests.size());
        Assert.assertEquals(2, scanner.next().getKey().byteAt(0) & 0xff);

        Assert.assertTrue(scanner.hasNext());
        Assert.assertEquals(2, requests.size());
        Assert.assertEquals(2, requests.get(1).getPageSize());
        Assert.assertEquals(3, scanner.next().getKey().byteAt(0) & 0xff);
        Assert.assertFalse(scanner.hasNext());
        Assert.assertEquals(2, requests.size());
    }

    @Test
    public void testRejectsOrderedScanWithoutCapabilityAck() {
        ScanStreamReq.Builder builder = ScanStreamReq.newBuilder()
                                                     .setMethod(ScanMethod.RANGE)
                                                     .setTable("table")
                                                     .setPageSize(2)
                                                     .setOrderType(
                                                             ScanOrderType.ORDER_BY_KEY);
        KvPageScanner scanner = new KvPageScanner(
                session(), builder, response -> onePageObserver(response, 0));

        try {
            scanner.hasNext();
            Assert.fail("Expected ordered scan capability failure");
        } catch (RuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("ordered scan"));
        }
    }

    @Test
    public void testAcceptsOrderedScanWithCapabilityAck() {
        ScanStreamReq.Builder builder = ScanStreamReq.newBuilder()
                                                     .setMethod(ScanMethod.RANGE)
                                                     .setTable("table")
                                                     .setPageSize(2)
                                                     .setOrderType(
                                                             ScanOrderType.ORDER_BY_KEY);
        KvPageScanner scanner = new KvPageScanner(
                session(), builder, response -> onePageObserver(response, 1));

        Assert.assertTrue(scanner.hasNext());
        Assert.assertEquals(1, scanner.next().getKey().byteAt(0) & 0xff);
        Assert.assertFalse(scanner.hasNext());
    }

    private static HgStoreNodeSession session() {
        return (HgStoreNodeSession) Proxy.newProxyInstance(
                HgStoreNodeSession.class.getClassLoader(),
                new Class[]{HgStoreNodeSession.class},
                (proxy, method, args) -> {
                    if ("getGraphName".equals(method.getName())) {
                        return "graph";
                    }
                    if ("isTx".equals(method.getName())) {
                        return false;
                    }
                    if ("toString".equals(method.getName())) {
                        return "TestNodeSession";
                    }
                    throw new UnsupportedOperationException(method.toString());
                });
    }

    private static StreamObserver<ScanStreamReq> requestObserver(
            List<ScanStreamReq> requests,
            StreamObserver<KvPageRes> response) {
        return new StreamObserver<ScanStreamReq>() {
            @Override
            public void onNext(ScanStreamReq request) {
                if (request.getCloseFlag() != 0) {
                    return;
                }
                requests.add(request);
                if (requests.size() == 1) {
                    response.onNext(page(false, 1, 2));
                } else if (requests.size() == 2) {
                    response.onNext(page(true, 3));
                } else {
                    Assert.fail("Unexpected page request");
                }
            }

            @Override
            public void onError(Throwable throwable) {
                throw new AssertionError(throwable);
            }

            @Override
            public void onCompleted() {
            }
        };
    }

    private static StreamObserver<ScanStreamReq> onePageObserver(
            StreamObserver<KvPageRes> response, int version) {
        return new StreamObserver<ScanStreamReq>() {
            @Override
            public void onNext(ScanStreamReq request) {
                if (request.getCloseFlag() == 0) {
                    response.onNext(versionedPage(true, version, 1));
                }
            }

            @Override
            public void onError(Throwable throwable) {
                // The client cancels the incompatible ordered stream
            }

            @Override
            public void onCompleted() {
            }
        };
    }

    private static KvPageRes page(boolean over, int... keys) {
        KvPageRes.Builder page = KvPageRes.newBuilder().setOver(over);
        for (int key : keys) {
            ByteString bytes = ByteString.copyFrom(new byte[]{(byte) key});
            page.addData(Kv.newBuilder().setKey(bytes).setValue(bytes));
        }
        return page.build();
    }

    private static KvPageRes versionedPage(boolean over, int version,
                                           int... keys) {
        return page(over, keys).toBuilder().setVersion(version).build();
    }
}
