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

package org.apache.hugegraph.ct.env;

import java.util.Arrays;

import org.apache.hugegraph.ct.node.AbstractNodeWrapper;
import org.junit.Assert;
import org.junit.Test;

public class AbstractEnvTest {

    @Test
    public void testCleanupFailureDoesNotSkipRemainingNodes() {
        TestNode first = new TestNode(true);
        TestNode second = new TestNode(false);
        TestNode third = new TestNode(true);
        AssertionError failure = Assert.assertThrows(AssertionError.class,
                () -> AbstractEnv.stopNodes(Arrays.asList(first, second, third)));
        Assert.assertTrue(first.stopped);
        Assert.assertTrue(second.stopped);
        Assert.assertTrue(third.stopped);
        Assert.assertEquals(1, failure.getSuppressed().length);
    }

    private static class TestNode extends AbstractNodeWrapper {

        private final boolean fail;
        private boolean stopped;

        private TestNode(boolean fail) {
            this.fail = fail;
        }

        @Override
        public void start() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void stop() {
            this.stopped = true;
            if (this.fail) {
                throw new AssertionError("Archive node logs failed");
            }
        }

        @Override
        public String getID() {
            return "test";
        }
    }
}
