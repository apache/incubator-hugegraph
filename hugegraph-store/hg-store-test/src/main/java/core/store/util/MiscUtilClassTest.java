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

package core.store.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.store.util.Asserts;
import org.apache.hugegraph.store.util.HgRaftError;
import org.apache.hugegraph.store.util.HgStoreException;
import org.apache.hugegraph.store.util.ManualResetEvent;
import org.junit.Test;

import com.alipay.sofa.jraft.Status;

public class MiscUtilClassTest {

    @Test
    public void testHgRaftError() {
        HgRaftError error = HgRaftError.forNumber(0);
        assertEquals(0, error.getNumber());
        assertEquals("OK", error.getMsg());
        assertEquals(Status.OK().getCode(), error.toStatus().getCode());
    }

    @Test(expected = NullPointerException.class)
    public void testAsserts() {
        assertTrue(Asserts.isInvalid(null));
        assertTrue(Asserts.isInvalid());
        assertTrue(Asserts.isInvalid(null));
        assertTrue(Asserts.isInvalid(""));
        assertFalse(Asserts.isInvalid("a"));

        Asserts.isNonNull(null);
        Asserts.isNonNull(null, "msg");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAsserts2() {
        Asserts.isTrue(false, "");
        Asserts.isFalse(true, "");
        Asserts.isTrue(true, null);
    }

    @Test
    public void testHgStoreException() {
        var exception = new HgStoreException();
        assertEquals(0, exception.getCode());
        exception = new HgStoreException("invalid");
        assertEquals(1000, exception.getCode());
        exception = new HgStoreException(1000, "invalid");
        assertEquals(1000, exception.getCode());
        exception = new HgStoreException(1000, new Throwable());
        assertEquals(1000, exception.getCode());
        exception = new HgStoreException("invalid", new Throwable());
        assertEquals(1000, exception.getCode());
        exception = new HgStoreException(1000, "%s", "invalid");
        assertEquals(1000, exception.getCode());
    }

    @Test
    public void testManualResetEvent() throws InterruptedException {
        ManualResetEvent event = new ManualResetEvent(false);
        assertFalse(event.isSignalled());
        event.set();
        assertTrue(event.isSignalled());
        event.reset();
        assertFalse(event.waitOne(1, TimeUnit.SECONDS));
        event.set();
        event.waitOne();
        assertTrue(event.isSignalled());
    }


}
