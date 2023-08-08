/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package core.raft;

import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.raft.RaftOperation;
import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.GeneratedMessageV3;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class RaftOperationTest {

    private RaftOperation raftOperationUnderTest;

    @Before
    public void setUp() {
        raftOperationUnderTest = new RaftOperation();
    }


    @Test
    public void testCreate1() {
        // Run the test
        final RaftOperation result = RaftOperation.create((byte) 0b0);
        assertEquals(null, result.getReq());
        assertEquals((byte) 0b0, result.getOp());
    }

    @Test
    public void testCreate2() {
        // Run the test
        final RaftOperation result = RaftOperation.create((byte) 0b0, "content".getBytes(), "req");
        assertArrayEquals("content".getBytes(), result.getValues());
        assertEquals("req", result.getReq());
        assertEquals((byte) 0b0, result.getOp());
    }

    @Test
    public void testCreate3() {
        // Run the test
        final RaftOperation result = RaftOperation.create((byte) 0b0, "req");
        assertEquals("req", result.getReq());
        assertEquals((byte) 0b0, result.getOp());
    }

    @Test
    public void testCreate4() throws Exception {
        // Setup
        final GeneratedMessageV3 req = Metapb.Graph.newBuilder().setGraphName("name").build();

        // Run the test
        final RaftOperation result = RaftOperation.create((byte) 0b0, req);
        assertEquals((byte) 0b0, result.getOp());

    }
}
