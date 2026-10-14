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

package org.apache.hugegraph.pd.core;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.apache.hugegraph.pd.RegistryService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.common.PDRuntimeException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfo;
import org.apache.hugegraph.pd.grpc.discovery.Query;
import org.apache.hugegraph.pd.meta.DiscoveryMetaStore;
import org.apache.hugegraph.pd.meta.MetadataFactory;
import org.apache.hugegraph.pd.store.HgKVStore;
import org.apache.hugegraph.pd.store.HgKVStoreImpl;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class RegistryIdentityTest {

    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();
    private HgKVStore previous;
    private HgKVStoreImpl store;
    private PDConfig config;
    private RegistryService registry;
    private String app;

    @Before
    public void open() throws Exception {
        this.config = new PDConfig();
        this.config.setDataPath(this.temporary.newFolder().getPath());
        this.config.setRaft(this.config.new Raft());
        this.store = new HgKVStoreImpl();
        this.store.init(this.config);
        this.previous = Whitebox.getInternalState(MetadataFactory.class, "store");
        Whitebox.setInternalState(MetadataFactory.class, "store", this.store);
        this.registry = new RegistryService(this.config);
        this.app = "identity-test-" + UUID.randomUUID();
    }

    @After
    public void close() throws Exception {
        try {
            // Remove only this fixture's TTL records; the TTL cache is shared across stores.
            DiscoveryMetaStore meta = new DiscoveryMetaStore(this.config);
            for (NodeInfo node : meta.getNodes(Query.newBuilder().setAppName(this.app).build())
                                     .getInfoList()) {
                remove(node);
            }
        } finally {
            Whitebox.setInternalState(MetadataFactory.class, "store", this.previous);
            this.store.close();
        }
    }

    private NodeInfo node(String address, String name, boolean full) {
        NodeInfo.Builder result = NodeInfo.newBuilder().setAppName(this.app).setVersion("v1")
                                         .setAddress(address).setInterval(60000)
                                         .putLabels("SERVICE_ID", "shared-id");
        if (full) {
            result.putLabels("GRAPHSPACE", "team_a").putLabels("SERVICE_NAME", name);
        }
        return result.build();
    }

    private Query byId() {
        return Query.newBuilder().setAppName(this.app).putLabels("SERVICE_ID", "shared-id").build();
    }

    private void remove(NodeInfo node) throws Exception {
        this.store.removeWithTTL(("REGIS-" + node.getAppName() + "-" + node.getVersion() + "-" +
                                  node.getAddress()).getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testCompleteTupleRejectsAliasAndPreservesReplicaVersions() throws Exception {
        NodeInfo original = node("one", "Foo", true);
        this.registry.register(original, 3);
        Assert.assertThrows(PDException.class, () -> this.registry.register(node("two", "foo", true), 3));
        this.registry.register(original.toBuilder().setAddress("replica").setVersion("v2").build(), 3);
        Assert.assertEquals(2, this.registry.getNodes(byId()).getInfoCount());
    }

    @Test
    public void testGenericOnlyPoolKeepsArbitraryLabels() throws Exception {
        this.registry.register(node("one", "", false), 3);
        this.registry.register(node("two", "", false).toBuilder().putLabels("any", "other").build(), 3);
        Assert.assertEquals(2, this.registry.getNodes(byId()).getInfoCount());
    }

    @Test
    public void testSamePhysicalKeyCannotDropIdentityOrChangeAlias() throws Exception {
        NodeInfo original = node("one", "Foo", true);
        this.registry.register(original, 3);
        Assert.assertThrows(PDException.class, () -> this.registry.register(node("one", "", false), 3));
        Assert.assertThrows(PDException.class, () -> this.registry.register(
                original.toBuilder().clearLabels().build(), 3));
        Assert.assertThrows(PDException.class, () -> this.registry.register(node("one", "foo", true), 3));
        Assert.assertEquals("Foo", this.registry.getNodes(byId()).getInfo(0).getLabelsOrThrow("SERVICE_NAME"));
    }

    @Test
    public void testGenericMustExitBeforeFirstCompleteTuple() throws Exception {
        NodeInfo generic = node("one", "", false);
        this.registry.register(generic, 3);
        Assert.assertThrows(PDException.class, () -> this.registry.register(node("one", "Foo", true), 3));
        Query raw = Query.newBuilder().setAppName(this.app).putLabels("GRAPHSPACE", "team_a")
                         .putLabels("SERVICE_NAME", "Foo").build();
        Assert.assertEquals(0, this.registry.getNodes(raw).getInfoCount());
        remove(generic);
        this.registry.register(node("one", "Foo", true), 3);
        Assert.assertEquals(1, this.registry.getNodes(raw).getInfoCount());
    }

    @Test
    public void testDirtyPoolRejectsIdQueryAndRenewalButKeepsEnumeration() throws Exception {
        // Seed actual legacy writes that predate the authority policy.
        DiscoveryMetaStore meta = new DiscoveryMetaStore(this.config);
        NodeInfo full = node("one", "Foo", true);
        meta.register(full, 3);
        meta.register(node("two", "", false).toBuilder().setVersion("v2").build(), 3);
        Assert.assertThrows(PDRuntimeException.class, () -> this.registry.getNodes(byId()));
        Assert.assertThrows(PDRuntimeException.class, () -> this.registry.getNodes(
                byId().toBuilder().setVersion("v1").build()));
        Assert.assertThrows(PDException.class, () -> this.registry.register(full, 3));
        Assert.assertEquals(2, this.registry.getNodes(Query.newBuilder().setAppName(this.app).build()).getInfoCount());
    }

    @Test
    public void testExpiredGenericDoesNotReserveIdentity() throws Exception {
        this.registry.register(node("one", "", false).toBuilder().setInterval(0).build(), 3);
        this.registry.register(node("two", "Foo", true), 3);
        Assert.assertEquals(1, this.registry.getNodes(byId()).getInfoCount());
    }

    @Test
    public void testCompleteEmptyValuesAreComparedWithoutNewInputRules() throws Exception {
        NodeInfo empty = node("one", "", true).toBuilder().putLabels("GRAPHSPACE", "").build();
        this.registry.register(empty, 3);
        this.registry.register(empty.toBuilder().setAddress("two").build(), 3);
        Assert.assertEquals(2, this.registry.getNodes(byId()).getInfoCount());
    }
}
