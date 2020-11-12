/*
 * Copyright 2017 HugeGraph Authors
 *
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

package com.baidu.hugegraph.auth;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.StandardHugeGraph;
import com.baidu.hugegraph.backend.cache.CacheManager;
import com.baidu.hugegraph.backend.tx.AbstractTransaction;
import com.baidu.hugegraph.concurrent.LockManager;
import com.baidu.hugegraph.license.LicenseVerifier;
import com.baidu.hugegraph.metrics.ServerReporter;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.schema.builder.EdgeLabelBuilder;
import com.baidu.hugegraph.schema.builder.IndexLabelBuilder;
import com.baidu.hugegraph.schema.builder.PropertyKeyBuilder;
import com.baidu.hugegraph.schema.builder.VertexLabelBuilder;
import com.baidu.hugegraph.serializer.JsonSerializer;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeProperty;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.task.HugeTask;
import com.baidu.hugegraph.task.StandardTaskScheduler;
import com.baidu.hugegraph.task.TaskCallable;
import com.baidu.hugegraph.task.TaskCallable.SysTaskCallable;
import com.baidu.hugegraph.task.TaskManager;
import com.baidu.hugegraph.traversal.optimize.HugeCountStepStrategy;
import com.baidu.hugegraph.traversal.optimize.HugeGraphStepStrategy;
import com.baidu.hugegraph.traversal.optimize.HugeVertexStepStrategy;
import com.baidu.hugegraph.variables.HugeVariables;
import com.google.common.collect.ImmutableSet;

import sun.reflect.Reflection;

public final class HugeFactoryAuthProxy {

    public static final String GRAPH_FACTORY =
           "gremlin.graph=com.baidu.hugegraph.auth.HugeFactoryAuthProxy";

    private static final Set<String> PROTECT_METHODS = ImmutableSet.of(
                                                       "instance");

    static {
        HugeGraphAuthProxy.setContext(HugeGraphAuthProxy.Context.admin());
        HugeFactoryAuthProxy.registerPrivateActions();
    }

    public static Graph open(Configuration config) {
        /*
         * Inject authentication (replace HugeGraph with HugeGraphAuthProxy)
         * TODO: Add verify to StandardHugeGraph() to prevent dynamic creation
         */
        return new HugeGraphAuthProxy(HugeFactory.open(config));
    }

    private static void registerPrivateActions() {
        // Thread
        Reflection.registerFieldsToFilter(java.lang.Thread.class, "name", "priority", "threadQ", "eetop", "single_step", "daemon", "stillborn", "target", "group", "contextClassLoader", "inheritedAccessControlContext", "threadInitNumber", "threadLocals", "inheritableThreadLocals", "stackSize", "nativeParkEventPointer", "tid", "threadSeqNumber", "threadStatus", "parkBlocker", "blocker", "blockerLock", "EMPTY_STACK_TRACE", "SUBCLASS_IMPLEMENTATION_PERMISSION", "uncaughtExceptionHandler", "defaultUncaughtExceptionHandler", "threadLocalRandomSeed", "threadLocalRandomProbe", "threadLocalRandomSecondarySeed");
        Reflection.registerMethodsToFilter(java.lang.Thread.class, "exit", "dispatchUncaughtException", "clone", "isInterrupted", "registerNatives", "init", "init", "nextThreadNum", "nextThreadID", "blockedOn", "start0", "isCCLOverridden", "auditSubclass", "dumpThreads", "getThreads", "processQueue", "setPriority0", "stop0", "suspend0", "resume0", "interrupt0", "setNativeName");
        Reflection.registerFieldsToFilter(java.lang.ThreadLocal.class, "threadLocalHashCode", "nextHashCode", "HASH_INCREMENT");
        Reflection.registerMethodsToFilter(java.lang.ThreadLocal.class, "access$400", "createInheritedMap", "nextHashCode", "initialValue", "setInitialValue", "getMap", "createMap", "childValue");
        Reflection.registerMethodsToFilter(java.lang.InheritableThreadLocal.class, "getMap", "createMap", "childValue");

        // HugeGraph
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.auth.StandardAuthenticator.class, "graph");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.auth.StandardAuthenticator.class, "graph", "initAdminUser", "inputPassword");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.auth.ConfigAuthenticator.class, "tokens");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.auth.HugeFactoryAuthProxy.class, "PROTECT_METHODS");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.auth.HugeFactoryAuthProxy.class, "c", "registerPrivateActions", "registerPrivateActions", "genRegisterPrivateActions", "registerClass");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.auth.HugeAuthenticator.User.class, "role", "client");
        Reflection.registerFieldsToFilter(org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser.class, "name");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.auth.HugeGraphAuthProxy.class, "LOG", "hugegraph", "taskScheduler", "userManager", "contexts", "$assertionsDisabled");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.auth.HugeGraphAuthProxy.class, "getContext", "lambda$0", "access$2", "access$3", "access$4", "access$5", "resetContext", "logUser", "setContext", "verifyAdminPermission", "verifyStatusPermission", "verifyPermission", "verifySchemaPermission", "verifySchemaPermission", "verifySchemaPermission", "verifySchemaPermission", "verifyNamePermission", "verifyNameExistsPermission", "verifyElemPermission", "verifyElemPermission", "verifyElemPermission", "verifyElemPermission", "verifyResPermission", "verifyUserPermission", "verifyUserPermission", "verifyUserPermission", "getContextString", "access$6", "access$7", "lambda$1", "lambda$2", "lambda$3", "lambda$4", "lambda$5", "lambda$6", "lambda$7", "lambda$8", "lambda$9", "lambda$10", "lambda$11", "lambda$12", "lambda$13", "lambda$14", "lambda$15", "lambda$16", "lambda$17", "lambda$18", "lambda$19", "lambda$20", "lambda$21", "lambda$22", "lambda$23", "lambda$24");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.auth.HugeGraphAuthProxy.UserManagerProxy.class, "userManager", "this$0");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.auth.HugeGraphAuthProxy.UserManagerProxy.class, "currentUsername", "updateCreator");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.auth.HugeGraphAuthProxy.TaskSchedulerProxy.class, "taskScheduler", "this$0");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.auth.HugeGraphAuthProxy.TaskSchedulerProxy.class, "verifyTaskPermission");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.auth.HugeGraphAuthProxy.GraphTraversalSourceProxy.class, "this$0");
        Reflection.registerFieldsToFilter(org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource.class, "connection", "graph", "strategies", "bytecode");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.auth.HugeGraphAuthProxy.TraversalStrategiesProxy.class, "serialVersionUID", "strategies", "this$0");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.auth.HugeGraphAuthProxy.TraversalStrategiesProxy.class, "translate");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.auth.HugeGraphAuthProxy.VariablesProxy.class, "variables", "this$0");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.auth.HugeGraphAuthProxy.Context.class, "ADMIN", "user");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.auth.HugeGraphAuthProxy.ContextTask.class, "runner", "context");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.StandardHugeGraph.class, "LOG", "closed", "mode", "variables", "name", "params", "configuration", "schemaEventHub", "graphEventHub", "indexEventHub", "writeRateLimiter", "readRateLimiter", "taskManager", "userManager", "features", "storeProvider", "tx");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.StandardHugeGraph.class, "waitUntilAllTasksCompleted", "lambda$0", "closeTx", "access$2", "access$3", "access$4", "access$5", "serializer", "loadSystemStore", "loadSchemaStore", "loadGraphStore", "analyzer", "access$8", "loadStoreProvider", "graphTransaction", "schemaTransaction", "openSchemaTransaction", "checkGraphNotClosed", "openSystemTransaction", "openGraphTransaction", "systemTransaction", "access$9", "access$10", "access$11", "access$12", "access$13", "access$14", "access$15", "access$16", "access$17", "access$18", "access$6", "access$7");
        Reflection.registerFieldsToFilter(c("com.baidu.hugegraph.StandardHugeGraph$StandardHugeGraphParams"), "graph", "this$0");
        Reflection.registerMethodsToFilter(c("com.baidu.hugegraph.StandardHugeGraph$StandardHugeGraphParams"), "graph", "access$1");
        Reflection.registerFieldsToFilter(c("com.baidu.hugegraph.StandardHugeGraph$TinkerPopTransaction"), "refs", "opened", "transactions", "this$0", "$assertionsDisabled");
        Reflection.registerMethodsToFilter(c("com.baidu.hugegraph.StandardHugeGraph$TinkerPopTransaction"), "resetState", "lambda$0", "access$1", "access$2", "access$3", "access$0", "setOpened", "doCommit", "verifyOpened", "doRollback", "doClose", "destroyTransaction", "graphTransaction", "schemaTransaction", "systemTransaction", "doOpen", "setClosed", "getOrNewTransaction", "lambda$1");
        Reflection.registerFieldsToFilter(org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction.class, "readWriteConsumerInternal", "closeConsumerInternal", "transactionListeners");
        Reflection.registerMethodsToFilter(org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction.class, "doClose", "fireOnCommit", "fireOnRollback", "doReadWrite", "lambda$fireOnRollback$1", "lambda$fireOnCommit$0");
        Reflection.registerFieldsToFilter(org.apache.tinkerpop.gremlin.structure.util.AbstractTransaction.class, "g");
        Reflection.registerMethodsToFilter(org.apache.tinkerpop.gremlin.structure.util.AbstractTransaction.class, "doCommit", "doRollback", "doClose", "fireOnCommit", "doOpen", "fireOnRollback", "doReadWrite");
        Reflection.registerFieldsToFilter(c("com.baidu.hugegraph.StandardHugeGraph$Txs"), "schemaTx", "systemTx", "graphTx", "$assertionsDisabled");
        Reflection.registerMethodsToFilter(c("com.baidu.hugegraph.StandardHugeGraph$Txs"), "access$1", "access$2", "access$0");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.backend.tx.GraphTransaction.class, "indexTx", "addedVertices", "removedVertices", "addedEdges", "removedEdges", "addedProps", "removedProps", "updatedVertices", "updatedEdges", "updatedOldestProps", "locksTable", "checkCustomVertexExist", "checkAdjacentVertexExist", "lazyLoadAdjacentVertex", "ignoreInvalidEntry", "commitPartOfAdjacentEdges", "batchSize", "pageSize", "verticesCapacity", "edgesCapacity", "$assertionsDisabled", "$SWITCH_TABLE$com$baidu$hugegraph$type$define$IdStrategy");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.backend.tx.GraphTransaction.class, "reset", "lambda$0", "access$1", "queryVertices", "indexTransaction", "indexTransaction", "beforeWrite", "prepareCommit", "$SWITCH_TABLE$com$baidu$hugegraph$type$define$IdStrategy", "lambda$1", "lambda$2", "lambda$3", "lambda$4", "lambda$5", "lambda$6", "lambda$7", "lambda$8", "lambda$9", "lambda$10", "lambda$11", "lambda$12", "lambda$13", "lambda$14", "lambda$15", "lambda$16", "lambda$17", "verticesInTxSize", "edgesInTxSize", "checkTxVerticesCapacity", "checkTxEdgesCapacity", "verticesInTxUpdated", "verticesInTxRemoved", "removingEdgeOwner", "prepareDeletions", "prepareDeletions", "prepareUpdates", "prepareAdditions", "checkVertexExistIfCustomizedId", "checkAggregateProperty", "checkAggregateProperty", "checkNonnullProperty", "queryEdgesFromBackend", "commitPartOfEdgeDeletions", "optimizeQueries", "checkVertexLabel", "checkId", "queryVerticesFromBackend", "joinTxVertices", "joinTxEdges", "lockForUpdateProperty", "verifyVerticesConditionQuery", "verifyEdgesConditionQuery", "optimizeQuery", "indexQuery", "filterResultFromIndexQuery", "joinTxRecords", "propertyUpdated", "parseEntry", "traverseByLabel");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.backend.tx.IndexableTransaction.class, "$assertionsDisabled");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.backend.tx.IndexableTransaction.class, "reset", "indexTransaction", "commit2Backend");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.backend.tx.AbstractTransaction.class, "LOG", "ownerThread", "autoCommit", "closed", "committing", "committing2Backend", "graph", "store", "mutation", "serializer", "$assertionsDisabled");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.backend.tx.AbstractTransaction.class, "store", "reset", "params", "autoCommit", "beforeRead", "beforeWrite", "afterWrite", "afterRead", "mutation", "commit2Backend", "prepareCommit", "commitMutation2Backend", "checkOwnerThread", "doAction");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.HugeFactory.class, "LOG", "NAME_REGEX", "graphs");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.HugeFactory.class, "lambda$0", "getLocalConfig", "getRemoteConfig");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.schema.SchemaElement.class, "graph", "id", "name", "userdata", "status");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.structure.HugeVertex.class, "tx", "label", "name", "edges", "$assertionsDisabled", "$SWITCH_TABLE$com$baidu$hugegraph$type$define$IdStrategy", "$SWITCH_TABLE$com$baidu$hugegraph$type$define$HugeKeys");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.structure.HugeVertex.class, "clone", "clone", "tx", "checkIdLength", "$SWITCH_TABLE$com$baidu$hugegraph$type$define$IdStrategy", "$SWITCH_TABLE$com$baidu$hugegraph$type$define$HugeKeys", "newProperty", "newProperty", "onUpdateProperty", "ensureFilledProperties");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.structure.HugeElement.class, "EMPTY", "MAX_PROPERTIES", "graph", "id", "properties", "removed", "fresh", "propLoaded", "$assertionsDisabled", "$SWITCH_TABLE$com$baidu$hugegraph$type$define$Cardinality");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.structure.HugeElement.class, "addProperty", "tx", "$SWITCH_TABLE$com$baidu$hugegraph$type$define$Cardinality", "getIdValue", "newProperty", "onUpdateProperty", "ensureFilledProperties");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.structure.HugeEdge.class, "label", "name", "sourceVertex", "targetVertex", "isOutEdge", "$assertionsDisabled", "$SWITCH_TABLE$com$baidu$hugegraph$type$define$HugeKeys", "$SWITCH_TABLE$org$apache$tinkerpop$gremlin$structure$Direction");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.structure.HugeEdge.class, "clone", "clone", "tx", "$SWITCH_TABLE$org$apache$tinkerpop$gremlin$structure$Direction", "checkAdjacentVertexExist", "$SWITCH_TABLE$com$baidu$hugegraph$type$define$HugeKeys", "newProperty", "newProperty", "onUpdateProperty", "ensureFilledProperties");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.structure.HugeProperty.class, "owner", "pkey", "value");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.variables.HugeVariables.class, "LOG", "VARIABLES", "VARIABLE_KEY", "VARIABLE_TYPE", "BYTE_VALUE", "BOOLEAN_VALUE", "INTEGER_VALUE", "LONG_VALUE", "FLOAT_VALUE", "DOUBLE_VALUE", "STRING_VALUE", "LIST", "SET", "TYPES", "params", "graph");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.variables.HugeVariables.class, "setProperty", "createPropertyKey", "queryAllVariableVertices", "queryVariableVertex", "createVariableVertex", "removeVariableVertex", "extractSingleObject");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.schema.SchemaManager.class, "transaction", "graph");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.schema.SchemaManager.class, "lambda$0", "checkExists", "lambda$1", "lambda$2", "lambda$3");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.schema.builder.PropertyKeyBuilder.class, "id", "name", "dataType", "cardinality", "aggregateType", "checkExist", "userdata", "$assertionsDisabled");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.schema.builder.PropertyKeyBuilder.class, "lambda$0", "checkStableVars", "checkAggregateType");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.schema.builder.AbstractBuilder.class, "transaction", "graph");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.schema.builder.AbstractBuilder.class, "rebuildIndex", "graph", "taskWaitTimeout", "checkSchemaName", "validOrGenerateId", "lockCheckAndCreateSchema", "propertyKeyOrNull", "checkSchemaIdIfRestoringMode", "vertexLabelOrNull", "edgeLabelOrNull", "indexLabelOrNull");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.schema.builder.VertexLabelBuilder.class, "id", "name", "idStrategy", "properties", "primaryKeys", "nullableKeys", "enableLabelIndex", "userdata", "checkExist", "$assertionsDisabled", "$SWITCH_TABLE$com$baidu$hugegraph$type$define$Action", "$SWITCH_TABLE$com$baidu$hugegraph$type$define$IdStrategy");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.schema.builder.VertexLabelBuilder.class, "lambda$0", "checkStableVars", "checkProperties", "checkNullableKeys", "checkIdStrategy", "checkPrimaryKeys", "$SWITCH_TABLE$com$baidu$hugegraph$type$define$IdStrategy", "$SWITCH_TABLE$com$baidu$hugegraph$type$define$Action");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.schema.builder.EdgeLabelBuilder.class, "id", "name", "sourceLabel", "targetLabel", "frequency", "properties", "sortKeys", "nullableKeys", "enableLabelIndex", "userdata", "checkExist", "$assertionsDisabled", "$SWITCH_TABLE$com$baidu$hugegraph$type$define$Action");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.schema.builder.EdgeLabelBuilder.class, "lambda$0", "checkSortKeys", "checkRelation", "checkStableVars", "checkProperties", "checkNullableKeys", "$SWITCH_TABLE$com$baidu$hugegraph$type$define$Action");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.schema.builder.IndexLabelBuilder.class, "id", "name", "baseType", "baseValue", "indexType", "indexFields", "userdata", "checkExist", "$assertionsDisabled", "$SWITCH_TABLE$com$baidu$hugegraph$type$HugeType", "$SWITCH_TABLE$com$baidu$hugegraph$type$define$DataType", "$SWITCH_TABLE$com$baidu$hugegraph$type$define$IndexType");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.schema.builder.IndexLabelBuilder.class, "lambda$0", "$SWITCH_TABLE$com$baidu$hugegraph$type$HugeType", "checkStableVars", "checkBaseType", "checkIndexType", "checkFields4Range", "loadElement", "loadElement", "checkFields", "checkRepeatIndex", "checkRepeatIndex", "checkRepeatIndex", "checkPrimaryKeyIndex", "checkRepeatRangeIndex", "checkRepeatSearchIndex", "checkRepeatSecondaryIndex", "checkRepeatShardIndex", "checkRepeatUniqueIndex", "removeSubIndex", "hasSubIndex", "allStringIndex", "oneNumericField", "$SWITCH_TABLE$com$baidu$hugegraph$type$define$IndexType", "$SWITCH_TABLE$com$baidu$hugegraph$type$define$DataType");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.task.TaskManager.class, "THREADS", "MANAGER", "schedulers", "taskExecutor", "dbExecutor", "contexts", "$assertionsDisabled");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.task.TaskManager.class, "lambda$0", "resetContext", "instance", "setContext", "closeTaskTx");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.task.StandardTaskScheduler.class, "graph", "taskExecutor", "dbExecutor", "eventListener", "tasks", "taskTx", "NO_LIMIT", "PAGE_SIZE", "QUERY_INTERVAL", "MAX_PENDING_TASKS", "$assertionsDisabled");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.task.StandardTaskScheduler.class, "remove", "lambda$0", "tx", "call", "call", "listenChanges", "unlistenChanges", "lambda$1", "lambda$2", "lambda$3", "lambda$4", "lambda$5", "lambda$6", "lambda$7", "submitTask", "initTaskCallable", "queryTask", "queryTask", "queryTask");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.task.HugeTask.class, "LOG", "scheduler", "callable", "type", "name", "id", "parent", "dependencies", "description", "context", "create", "status", "progress", "update", "retries", "input", "result", "$assertionsDisabled");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.task.HugeTask.class, "set", "result", "status", "scheduler", "scheduler", "property", "callable", "setException", "done", "asArray", "checkPropertySize", "checkPropertySize", "checkDependenciesSuccess", "toOrderSet");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.task.TaskCallable.class, "LOG", "ERROR_MAX_LEN", "task", "graph", "lastSaveTime", "saveInterval");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.task.TaskCallable.class, "save", "graph", "task", "done", "cancelled");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.task.TaskCallable.SysTaskCallable.class, "params");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.task.TaskCallable.SysTaskCallable.class, "params", "params");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.auth.StandardUserManager.class, "CACHE_EXPIRE", "graph", "eventListener", "usersCache", "users", "groups", "targets", "belong", "access", "$assertionsDisabled");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.auth.StandardUserManager.class, "cache", "lambda$0", "invalidCache", "initSchemaIfNeeded", "listenChanges", "unlistenChanges", "rolePermission", "rolePermission");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.auth.SchemaDefine.class, "graph", "label");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.auth.SchemaDefine.class, "initProperties", "schema", "existEdgeLabel", "createRangeIndex", "unhideField", "hideField", "createPropertyKey", "createPropertyKey", "createPropertyKey", "existVertexLabel");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.auth.EntityManager.class, "graph", "label", "deser", "NO_LIMIT", "$assertionsDisabled");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.auth.EntityManager.class, "save", "query", "graph", "tx", "toList", "commitOrRollback", "unhideLabel", "queryById", "queryEntity", "constructVertex");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.auth.RelationshipManager.class, "graph", "label", "deser", "NO_LIMIT", "$assertionsDisabled");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.auth.RelationshipManager.class, "save", "queryRelationship", "lambda$0", "graph", "tx", "toList", "commitOrRollback", "unhideLabel", "queryById", "newVertex");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.auth.HugeUser.class, "name", "password", "phone", "email", "avatar", "role");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.auth.HugeUser.class, "property", "asArray");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.auth.SchemaDefine.UserElement.class, "CREATE", "UPDATE", "CREATOR", "id", "create", "update", "creator");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.auth.SchemaDefine.UserElement.class, "asMap", "property", "asArray", "asArray");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.auth.HugeResource.class, "CHECK_NAME_RESS", "type", "label", "properties", "$assertionsDisabled");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.auth.HugeResource.class, "contains", "filter", "filter", "filter", "access$1", "access$2", "access$3", "access$4", "access$5", "access$0", "matchLabel", "matchProperties");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.auth.RolePermission.class, "map");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.auth.RolePermission.class, "add", "add", "contains", "map", "access$0");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.backend.cache.CacheManager.class, "LOG", "INSTANCE", "TIMER_TICK_PERIOD", "LOG_TICK_COST_TIME", "caches", "timer");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.backend.cache.CacheManager.class, "access$0", "instance", "scheduleTimer");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.concurrent.LockManager.class, "INSTANCE", "lockGroupMap");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.concurrent.LockManager.class, "instance");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.license.LicenseVerifier.class, "LOG", "LICENSE_PARAM_PATH", "INSTANCE", "CHECK_INTERVAL", "lastCheckTime", "verifyParam", "manager");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.license.LicenseVerifier.class, "instance", "buildVerifyParam", "initLicenseParam", "verifyPublicCert");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.metrics.ServerReporter.class, "instance", "gauges", "counters", "histograms", "meters", "timers");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.metrics.ServerReporter.class, "instance", "instance");
        Reflection.registerFieldsToFilter(com.codahale.metrics.ScheduledReporter.class, "registry", "executor", "filter", "durationFactor", "durationUnit", "rateFactor", "rateUnit");
        Reflection.registerMethodsToFilter(com.codahale.metrics.ScheduledReporter.class, "calculateRateUnit", "convertDuration", "convertRate", "getRateUnit", "getDurationUnit");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.serializer.JsonSerializer.class, "LBUF_SIZE", "INSTANCE");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.serializer.JsonSerializer.class, "instance", "writeIterator");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.traversal.optimize.HugeVertexStepStrategy.class, "serialVersionUID", "INSTANCE");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.traversal.optimize.HugeVertexStepStrategy.class, "instance");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.traversal.optimize.HugeGraphStepStrategy.class, "serialVersionUID", "INSTANCE");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.traversal.optimize.HugeGraphStepStrategy.class, "instance");
        Reflection.registerFieldsToFilter(com.baidu.hugegraph.traversal.optimize.HugeCountStepStrategy.class, "serialVersionUID", "INSTANCE");
        Reflection.registerMethodsToFilter(com.baidu.hugegraph.traversal.optimize.HugeCountStepStrategy.class, "lambda$0", "instance");

        // Enable this line to generate registration statement
        //genRegisterPrivateActions();
    }

    @SuppressWarnings("unused")
    private static void genRegisterPrivateActions() {
        registerPrivateActions(Thread.class);
        registerPrivateActions(ThreadLocal.class);
        registerPrivateActions(InheritableThreadLocal.class);

        registerPrivateActions(StandardAuthenticator.class);
        registerPrivateActions(ConfigAuthenticator.class);
        registerPrivateActions(HugeFactoryAuthProxy.class);
        registerPrivateActions(HugeAuthenticator.User.class);

        registerPrivateActions(HugeGraphAuthProxy.class);
        registerPrivateActions(HugeGraphAuthProxy.UserManagerProxy.class);
        registerPrivateActions(HugeGraphAuthProxy.TaskSchedulerProxy.class);
        registerPrivateActions(HugeGraphAuthProxy.GraphTraversalSourceProxy.class);
        registerPrivateActions(HugeGraphAuthProxy.TraversalStrategiesProxy.class);
        registerPrivateActions(HugeGraphAuthProxy.VariablesProxy.class);
        registerPrivateActions(HugeGraphAuthProxy.Context.class);
        registerPrivateActions(HugeGraphAuthProxy.ContextThreadPoolExecutor.class);
        registerPrivateActions(HugeGraphAuthProxy.ContextTask.class);

        for (Class<?> clazz : StandardHugeGraph.PROTECT_CLASSES) {
            registerPrivateActions(clazz);
        }

        registerPrivateActions(HugeFactory.class);
        registerPrivateActions(AbstractTransaction.class);

        registerPrivateActions(SchemaElement.class);
        registerPrivateActions(HugeVertex.class);
        registerPrivateActions(HugeEdge.class);
        registerPrivateActions(HugeProperty.class);
        registerPrivateActions(HugeVariables.class);

        registerPrivateActions(SchemaManager.class);
        registerPrivateActions(PropertyKeyBuilder.class);
        registerPrivateActions(VertexLabelBuilder.class);
        registerPrivateActions(EdgeLabelBuilder.class);
        registerPrivateActions(IndexLabelBuilder.class);

        registerPrivateActions(TaskManager.class);
        registerPrivateActions(StandardTaskScheduler.class);
        registerPrivateActions(HugeTask.class);
        registerPrivateActions(TaskCallable.class);
        registerPrivateActions(SysTaskCallable.class);

        registerPrivateActions(StandardUserManager.class);
        registerPrivateActions(SchemaDefine.class);
        registerPrivateActions(EntityManager.class);
        registerPrivateActions(RelationshipManager.class);
        registerPrivateActions(HugeUser.class);
        registerPrivateActions(HugeResource.class);
        registerPrivateActions(RolePermission.class);

        registerPrivateActions(CacheManager.class);
        registerPrivateActions(LockManager.class);
        registerPrivateActions(LicenseVerifier.class);
        registerPrivateActions(ServerReporter.class);
        registerPrivateActions(JsonSerializer.class);
        registerPrivateActions(HugeVertexStepStrategy.class);
        registerPrivateActions(HugeGraphStepStrategy.class);
        registerPrivateActions(HugeCountStepStrategy.class);
    }

    private static void registerPrivateActions(Class<?> clazz) {
        while (clazz != Object.class) {
            List<String> fields = new ArrayList<>();
            for (Field field : clazz.getDeclaredFields()) {
                if (!Modifier.isPublic(field.getModifiers())) {
                    fields.add(field.getName());
                }
            }
            List<String> methods = new ArrayList<>();
            for (Method method : clazz.getDeclaredMethods()) {
                if (!Modifier.isPublic(method.getModifiers()) ||
                    PROTECT_METHODS.contains(method.getName())) {
                    methods.add(method.getName());
                }
            }
            registerClass(clazz, fields, methods);
            clazz = clazz.getSuperclass();
        }
    }

    private static boolean registerClass(Class<?> clazz,
                                         List<String> fields,
                                         List<String> methods) {
        if (clazz.getName().startsWith("java") ||
            fields.isEmpty() && methods.isEmpty()) {
            return false;
        }
        final String[] array = new String[fields.size()];
        try {
            Reflection.registerFieldsToFilter(clazz, fields.toArray(array));
            Reflection.registerMethodsToFilter(clazz, methods.toArray(array));
        } catch (IllegalArgumentException e) {
            if (e.getMessage().contains("Filter already registered: class")) {
                return false;
            }
            throw e;
        }

        String code;
        code = String.format("Reflection.registerFieldsToFilter(%s.class, \"%s\");",
                             clazz.getCanonicalName(), String.join("\", \"", fields));
        if (!fields.isEmpty()) {
            System.out.println(code);
        }

        code = String.format("Reflection.registerMethodsToFilter(%s.class, \"%s\");",
                             clazz.getCanonicalName(), String.join("\", \"", methods));
        if (!methods.isEmpty()) {
            System.out.println(code);
        }

        return true;
    }

    private static Class<?> c(String clazz) {
        try {
            return Class.forName(clazz);
        } catch (ClassNotFoundException e) {
            throw new HugeException(e.getMessage(), e);
        }
    }
}
