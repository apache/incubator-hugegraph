/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.hugegraph.unit.mysql;

import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.backend.store.mysql.MysqlSessions;
import org.apache.hugegraph.backend.store.mysql.ResultSetWrapper;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;

public class MysqlTest extends BaseMysqlUnitTest {

    @Before
    public void setUp() throws Exception {
        super.setup();
    }

    @Test
    public void testMysqlMetaVersion() throws SQLException {
        BackendStore systemStore = this.provider.loadSystemStore(config);

        // Record system version before truncation
        String beforeVersion = systemStore.storedVersion();

        // Get sessions for direct table operations
        MysqlSessions.Session session = this.sessions.session();
        session.open();

        // Get the actual table names from the store
        String vertexTable = "g_v";
        String edgeOutTable = "g_oe";
        String edgeInTable = "g_ie";

        // Insert test vertex data directly using simple SQL
        String insertVertex = String.format(
                "INSERT INTO %s (ID, LABEL, PROPERTIES, EXPIRED_TIME) VALUES ('test_vertex', 1, " +
                "'{\"name\":\"test_vertex\"}', 0)",
                vertexTable
        );
        session.execute(insertVertex);

        // Insert test edge data directly using simple SQL
        String insertEdgeOut = String.format(
                "INSERT INTO %s (OWNER_VERTEX, DIRECTION, LABEL, SUB_LABEL, SORT_VALUES, " +
                "OTHER_VERTEX, PROPERTIES, EXPIRED_TIME) VALUES ('test_vertex', 0, 1, 0, '', " +
                "'target_vertex', '{\"weight\":1.0}', 0)",
                edgeOutTable
        );
        session.execute(insertEdgeOut);

        String insertEdgeIn = String.format(
                "INSERT INTO %s (OWNER_VERTEX, DIRECTION, LABEL, SUB_LABEL, SORT_VALUES, " +
                "OTHER_VERTEX, PROPERTIES, EXPIRED_TIME) VALUES ('target_vertex', 1, 1, 0, '', " +
                "'test_vertex', '{\"weight\":1.0}', 0)",
                edgeInTable
        );
        session.execute(insertEdgeIn);

        // Verify data exists by querying
        String selectVertex = String.format("SELECT * FROM %s WHERE ID = 'test_vertex'", vertexTable);
        String selectEdgeOut = String.format(
                "SELECT * FROM %s WHERE OWNER_VERTEX = 'test_vertex' AND DIRECTION = 0",
                edgeOutTable);
        String selectEdgeIn = String.format(
                "SELECT * FROM %s WHERE OWNER_VERTEX = 'target_vertex' AND DIRECTION = 1",
                edgeInTable);
        try (
                ResultSetWrapper vResult = session.select(selectVertex);
                ResultSetWrapper oeResult = session.select(selectEdgeOut);
                ResultSetWrapper ieResult = session.select(selectEdgeIn);
        ) {
            Assert.assertTrue("vertex data should exist", vResult.next());
            Assert.assertTrue("out edge data should exist", oeResult.next());
            Assert.assertTrue("in edge data should exist", ieResult.next());
        }

        // Execute truncate operation, clears all graph data but preserves system tables
        this.provider.truncate();

        // Verify system version remains unchanged after truncation
        String afterVersion = systemStore.storedVersion();
        Assert.assertNotNull("System metadata version should exist", afterVersion);
        Assert.assertEquals("System metadata version should remain unchanged after truncation",
                            beforeVersion, afterVersion);

        // Verify data has been cleared
        try (
                ResultSetWrapper vResult = session.select(selectVertex);
                ResultSetWrapper oeResult = session.select(selectEdgeOut);
                ResultSetWrapper ieResult = session.select(selectEdgeIn);
        ) {
            Assert.assertFalse("vertex data should not exist", vResult.next());
            Assert.assertFalse("out edge data should not exist", oeResult.next());
            Assert.assertFalse("in edge data should not exist", ieResult.next());
        }
    }
}
