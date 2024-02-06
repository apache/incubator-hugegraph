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

package org.apache.hugegraph.backend.store.postgresql;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.util.Strings;

import org.apache.hugegraph.backend.serializer.TableBackendEntry.Row;
import org.apache.hugegraph.backend.store.mysql.MysqlBackendEntry;
import org.apache.hugegraph.backend.store.mysql.MysqlSessions.Session;
import org.apache.hugegraph.backend.store.mysql.MysqlTable;
import org.apache.hugegraph.backend.store.mysql.WhereBuilder;
import org.apache.hugegraph.type.define.HugeKeys;

public abstract class PostgresqlTable extends MysqlTable {

    private String orderByKeysTemplate = null;

    public PostgresqlTable(String table) {
        super(table);
    }

    @Override
    protected String buildDropTemplate() {
        return String.format("DROP TABLE IF EXISTS %s CASCADE;", this.table());
    }

    @Override
    protected String buildTruncateTemplate() {
        return String.format("TRUNCATE TABLE %s CASCADE;", this.table());
    }

    @Override
    protected String engine(Session session) {
        return Strings.EMPTY;
    }

    @Override
    protected String buildUpdateForcedTemplate(MysqlBackendEntry.Row entry) {
        return this.buildInsertKeys(entry, false);
    }

    @Override
    protected List<?> buildUpdateForcedParams(MysqlBackendEntry.Row entry) {
        List<Object> params = new ArrayList<>();
        List<Object> allColumns = this.buildColumnsParams(entry);
        params.addAll(allColumns);
        params.addAll(allColumns);
        return params;
    }

    @Override
    protected String buildUpdateIfAbsentTemplate(Row entry) {
        return this.buildInsertKeys(entry, true);
    }

    @Override
    protected List<?> buildUpdateIfAbsentParams(MysqlBackendEntry.Row entry) {
        return this.buildColumnsParams(entry);
    }

    protected String buildInsertKeys(MysqlBackendEntry.Row entry,
                                     boolean ignoreConflicts) {
        StringBuilder insert = new StringBuilder();
        insert.append("INSERT INTO ").append(this.table()).append(" (");

        int i = 0;
        int size = entry.columns().size();
        for (HugeKeys key : entry.columns().keySet()) {
            insert.append(formatKey(key));
            if (++i != size) {
                insert.append(", ");
            }
        }
        insert.append(") VALUES (");

        for (i = 0; i < size; i++) {
            insert.append("?");
            if (i != size - 1) {
                insert.append(", ");
            }
        }
        insert.append(")");

        i = 0;
        size = this.tableDefine().keys().size();
        insert.append(" ON CONFLICT (");
        for (HugeKeys key : this.tableDefine().keys()) {
            insert.append(formatKey(key));
            if (++i != size) {
                insert.append(", ");
            }
        }
        insert.append(")");

        if (ignoreConflicts) {
            insert.append(" DO NOTHING");
        } else {
            i = 0;
            size = entry.columns().keySet().size();
            insert.append(" DO UPDATE SET ");
            for (HugeKeys key : entry.columns().keySet()) {
                insert.append(formatKey(key)).append(" = ?");
                if (++i != size) {
                    insert.append(", ");
                }
            }
        }

        return insert.toString();
    }

    @Override
    protected String orderByKeys() {
        // Set order-by to keep results order consistence for PostgreSQL result
        if (this.orderByKeysTemplate != null) {
            return this.orderByKeysTemplate;
        }
        int i = 0;
        int size = this.tableDefine().keys().size();
        StringBuilder select = new StringBuilder(" ORDER BY ");
        for (HugeKeys hugeKey : this.tableDefine().keys()) {
            String key = formatKey(hugeKey);
            select.append(key).append(" ");
            select.append("ASC ");
            if (++i != size) {
                select.append(", ");
            }
        }
        this.orderByKeysTemplate = select.toString();
        return this.orderByKeysTemplate;
    }

    @Override
    protected WhereBuilder newWhereBuilder(boolean startWithWhere) {
        return new PgWhereBuilder(startWithWhere);
    }

    private static class PgWhereBuilder extends WhereBuilder {

        public PgWhereBuilder(boolean startWithWhere) {
            super(startWithWhere);
        }

        @Override
        protected String escapeAndWrapString(String value) {
            if (value.equals("\u0000")) {
                return "\'\'";
            }
            return PostgresqlSessions.escapeAndWrapString(value);
        }
    }
}
