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

package org.apache.hugegraph.security;

import java.io.FileDescriptor;
import java.net.InetAddress;
import java.security.Permission;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.slf4j.Logger;

import org.apache.hugegraph.util.Log;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class HugeSecurityManager extends SecurityManager {

    private static final String USER_DIR = System.getProperty("user.dir");

    private static final String USER_DIR_IDE =
                                USER_DIR.endsWith("hugegraph-dist") ?
                                USER_DIR.substring(0, USER_DIR.length() - 15) :
                                null;

    private static final String GREMLIN_SERVER_WORKER = "gremlin-server-exec";
    private static final String TASK_WORKER = "task-worker";
    private static final Set<String> GREMLIN_EXECUTOR_CLASS = ImmutableSet.of(
            "org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine"
    );

    // TODO: add "suppressAccessChecks"
    private static final Set<String> DENIED_PERMISSIONS = ImmutableSet.of("setSecurityManager");



    private static final Set<String> ACCEPT_CLASS_LOADERS = ImmutableSet.of(
            "groovy.lang.GroovyClassLoader",
            "sun.reflect.DelegatingClassLoader",
            "jdk.internal.reflect.DelegatingClassLoader",
            "org.codehaus.groovy.reflection.SunClassLoader",
            "org.codehaus.groovy.runtime.callsite.CallSiteClassLoader",
            "org.apache.hadoop.hbase.util.DynamicClassLoader",
            "org.apache.tinkerpop.gremlin.groovy.loaders.GremlinLoader"
    );

    private static final Set<String> CAFFEINE_CLASSES = ImmutableSet.of(
            "com.github.benmanes.caffeine.cache.BoundedLocalCache"
    );

    private static final Set<String> WHITE_SYSTEM_PROPERTIES = ImmutableSet.of(
            "line.separator",
            "file.separator",
            "java.specification.version", // Sofa
            "socksProxyHost", // MySQL
            "file.encoding" // PostgreSQL
    );

    private static final Map<String, Set<String>> ASYNC_TASKS = ImmutableMap.of(
            // Fixed https://github.com/apache/hugegraph/pull/892#issue-387202362
            "org.apache.hugegraph.backend.tx.SchemaTransaction",
            ImmutableSet.of("removeVertexLabel", "removeEdgeLabel",
                            "removeIndexLabel", "rebuildIndex"),
            "org.apache.hugegraph.backend.tx.GraphIndexTransaction",
            ImmutableSet.of("asyncRemoveIndexLeft")
    );

    private static final Map<String, Set<String>> BACKEND_SOCKET = ImmutableMap.of(
            // Fixed #758
            "org.apache.hugegraph.backend.store.mysql.MysqlStore",
            ImmutableSet.of("open", "init", "clear", "opened", "initialized")
    );

    private static final Map<String, Set<String>> BACKEND_THREAD = ImmutableMap.of(
            // Fixed #758
            "org.apache.hugegraph.backend.store.cassandra.CassandraStore",
            ImmutableSet.of("open", "opened", "init"),
            // Fixed https://github.com/apache/hugegraph/pull/892#issuecomment-598545072
            "com.datastax.driver.core.AbstractSession",
            ImmutableSet.of("execute")
    );

    private static final Map<String, Set<String>> BACKEND_SNAPSHOT = ImmutableMap.of(
            "org.apache.hugegraph.backend.store.AbstractBackendStoreProvider",
            ImmutableSet.of("createSnapshot", "resumeSnapshot"),
            "org.apache.hugegraph.backend.store.raft.RaftBackendStoreProvider",
            ImmutableSet.of("createSnapshot", "resumeSnapshot")
    );

    private static final Set<String> HBASE_CLASSES = ImmutableSet.of(
            // Fixed #758
            "org.apache.hugegraph.backend.store.hbase.HbaseStore",
            "org.apache.hugegraph.backend.store.hbase.HbaseStore$HbaseSchemaStore",
            "org.apache.hugegraph.backend.store.hbase.HbaseStore$HbaseGraphStore",
            "org.apache.hugegraph.backend.store.hbase.HbaseSessions$RowIterator"
    );

    private static final Set<String> RAFT_CLASSES = ImmutableSet.of(
            "org.apache.hugegraph.backend.store.raft.RaftNode",
            "org.apache.hugegraph.backend.store.raft.StoreStateMachine",
            "org.apache.hugegraph.backend.store.raft.rpc.RpcForwarder"
    );

    private static final Set<String> SOFA_RPC_CLASSES = ImmutableSet.of(
            "com.alipay.sofa.rpc.tracer.sofatracer.RpcSofaTracer",
            "com.alipay.sofa.rpc.client.AbstractCluster"
    );

    private static final Map<String, Set<String>> NEW_SECURITY_EXCEPTION = ImmutableMap.of(
            "org.apache.hugegraph.security.HugeSecurityManager",
            ImmutableSet.of("newSecurityException")
    );

    private static final Set<String> IGNORE_CHECKED_CLASSES = new CopyOnWriteArraySet<>();

    public static void ignoreCheckedClass(String clazz) {
        if (callFromGremlin()) {
            throw newSecurityException(
                  "Not allowed to add ignore check via Gremlin");
        }

        IGNORE_CHECKED_CLASSES.add(clazz);
    }

    @Override
    public void checkPermission(Permission permission) {
        if (DENIED_PERMISSIONS.contains(permission.getName()) &&
            callFromGremlin()) {
            throw newSecurityException(
                  "Not allowed to access denied permission via Gremlin");
        }
    }

    @Override
    public void checkPermission(Permission permission, Object context) {
        if (DENIED_PERMISSIONS.contains(permission.getName()) &&
            callFromGremlin()) {
            throw newSecurityException(
                  "Not allowed to access denied permission via Gremlin");
        }
    }

    @Override
    public void checkCreateClassLoader() {
        if (!callFromAcceptClassLoaders() && callFromGremlin()) {
            throw newSecurityException(
                  "Not allowed to create class loader via Gremlin");
        }
        super.checkCreateClassLoader();
    }

    @Override
    public void checkLink(String lib) {
        if (callFromGremlin()) {
            throw newSecurityException(
                  "Not allowed to link library via Gremlin");
        }
        super.checkLink(lib);
    }

    @Override
    public void checkAccess(Thread thread) {
        if (callFromGremlin() && !callFromCaffeine() &&
            !callFromAsyncTasks() && !callFromEventHubNotify() &&
            !callFromBackendThread() && !callFromBackendHbase() &&
            !callFromRaft() && !callFromSofaRpc() && !callFromIgnoreCheckedClass()) {
            throw newSecurityException(
                  "Not allowed to access thread via Gremlin");
        }
        super.checkAccess(thread);
    }

    @Override
    public void checkAccess(ThreadGroup threadGroup) {
        if (callFromGremlin() && !callFromCaffeine() &&
            !callFromAsyncTasks() && !callFromEventHubNotify() &&
            !callFromBackendThread() && !callFromBackendHbase() &&
            !callFromRaft() && !callFromSofaRpc() &&
            !callFromIgnoreCheckedClass()) {
            throw newSecurityException(
                  "Not allowed to access thread group via Gremlin");
        }
        super.checkAccess(threadGroup);
    }

    @Override
    public void checkExit(int status) {
        if (callFromGremlin()) {
            throw newSecurityException(
                  "Not allowed to call System.exit() via Gremlin");
        }
        super.checkExit(status);
    }

    @Override
    public void checkExec(String cmd) {
        if (callFromGremlin()) {
            throw newSecurityException(
                  "Not allowed to execute command via Gremlin");
        }
        super.checkExec(cmd);
    }

    @Override
    public void checkRead(FileDescriptor fd) {
        if (callFromGremlin() && !callFromBackendSocket() &&
            !callFromRaft() && !callFromSofaRpc()) {
            throw newSecurityException("Not allowed to read fd via Gremlin");
        }
        super.checkRead(fd);
    }

    @Override
    public void checkRead(String file) {
        if (callFromGremlin() && !callFromCaffeine() &&
            !readGroovyInCurrentDir(file) && !callFromBackendHbase() &&
            !callFromSnapshot() && !callFromRaft() &&
            !callFromSofaRpc()) {
            throw newSecurityException(
                  "Not allowed to read file via Gremlin: %s", file);
        }
        super.checkRead(file);
    }

    @Override
    public void checkRead(String file, Object context) {
        if (callFromGremlin() && !callFromRaft() && !callFromSofaRpc()) {
            throw newSecurityException(
                  "Not allowed to read file via Gremlin: %s", file);
        }
        super.checkRead(file, context);
    }

    @Override
    public void checkWrite(FileDescriptor fd) {
        if (callFromGremlin() && !callFromBackendSocket() &&
            !callFromRaft() && !callFromSofaRpc()) {
            throw newSecurityException("Not allowed to write fd via Gremlin");
        }
        super.checkWrite(fd);
    }

    @Override
    public void checkWrite(String file) {
        if (callFromGremlin() && !callFromSnapshot() &&
            !callFromRaft() && !callFromSofaRpc()) {
            throw newSecurityException("Not allowed to write file via Gremlin");
        }
        super.checkWrite(file);
    }

    @Override
    public void checkDelete(String file) {
        if (callFromGremlin() && !callFromSnapshot()) {
            throw newSecurityException(
                  "Not allowed to delete file via Gremlin");
        }
        super.checkDelete(file);
    }

    @Override
    public void checkListen(int port) {
        if (callFromGremlin()) {
            throw newSecurityException(
                  "Not allowed to listen socket via Gremlin");
        }
        super.checkListen(port);
    }

    @Override
    public void checkAccept(String host, int port) {
        if (callFromGremlin()) {
            throw newSecurityException(
                  "Not allowed to accept socket via Gremlin");
        }
        super.checkAccept(host, port);
    }

    @Override
    public void checkConnect(String host, int port) {
        if (callFromGremlin() && !callFromBackendSocket() &&
            !callFromBackendHbase() && !callFromRaft() && !callFromSofaRpc()) {
            throw newSecurityException(
                  "Not allowed to connect socket via Gremlin");
        }
        super.checkConnect(host, port);
    }

    @Override
    public void checkConnect(String host, int port, Object context) {
        if (callFromGremlin()) {
            throw newSecurityException(
                  "Not allowed to connect socket via Gremlin");
        }
        super.checkConnect(host, port, context);
    }

    @Override
    public void checkMulticast(InetAddress maddr) {
        if (callFromGremlin()) {
            throw newSecurityException("Not allowed to multicast via Gremlin");
        }
        super.checkMulticast(maddr);
    }

    @Override
    @SuppressWarnings("deprecation")
    public void checkMulticast(InetAddress maddr, byte ttl) {
        if (callFromGremlin()) {
            throw newSecurityException("Not allowed to multicast via Gremlin");
        }
        super.checkMulticast(maddr, ttl);
    }

    @Override
    public void checkSetFactory() {
        if (callFromGremlin()) {
            throw newSecurityException(
                  "Not allowed to set socket factory via Gremlin");
        }
        super.checkSetFactory();
    }

    @Override
    public void checkPropertiesAccess() {
        if (callFromGremlin() && !callFromSofaRpc() &&
            !callFromNewSecurityException()) {
            throw newSecurityException(
                  "Not allowed to access system properties via Gremlin");
        }
        super.checkPropertiesAccess();
    }

    @Override
    public void checkPropertyAccess(String key) {
        if (!callFromAcceptClassLoaders() && callFromGremlin() &&
            !WHITE_SYSTEM_PROPERTIES.contains(key) && !callFromBackendHbase() &&
            !callFromSnapshot() && !callFromRaft() &&
            !callFromSofaRpc()) {
            throw newSecurityException(
                  "Not allowed to access system property(%s) via Gremlin", key);
        }
        super.checkPropertyAccess(key);
    }

    @Override
    public void checkPrintJobAccess() {
        if (callFromGremlin()) {
            throw newSecurityException("Not allowed to print job via Gremlin");
        }
        super.checkPrintJobAccess();
    }

    @Override
    public void checkPackageAccess(String pkg) {
        super.checkPackageAccess(pkg);
    }

    @Override
    public void checkPackageDefinition(String pkg) {
        super.checkPackageDefinition(pkg);
    }

    @Override
    public void checkSecurityAccess(String target) {
        super.checkSecurityAccess(target);
    }

    private static SecurityException newSecurityException(String message,
                                                          Object... args) {
        if (args.length > 0) {
            message = String.format(message, args);
        }
        /*
         * use dynamic logger here because "static final logger" can't be
         * initialized: the logger is not initialized when HugeSecurityManager
         * class is loaded
         */
        Logger log = Log.logger(HugeSecurityManager.class);
        log.warn("SecurityException: {}", message);
        return new SecurityException(message);
    }

    private static boolean readGroovyInCurrentDir(String file) {
        if (file != null && (USER_DIR != null && file.startsWith(USER_DIR) ||
            USER_DIR_IDE != null && file.startsWith(USER_DIR_IDE)) &&
            (file.endsWith(".class") || file.endsWith(".groovy"))) {
            return true;
        }
        return false;
    }

    private static boolean callFromGremlin() {
        return callFromWorkerWithClass(GREMLIN_EXECUTOR_CLASS);
    }

    private static boolean callFromAcceptClassLoaders() {
        return callFromWorkerWithClass(ACCEPT_CLASS_LOADERS);
    }

    private static boolean callFromCaffeine() {
        return callFromWorkerWithClass(CAFFEINE_CLASSES);
    }

    private static boolean callFromBackendSocket() {
        // Fixed issue #758
        return callFromMethods(BACKEND_SOCKET);
    }

    private static boolean callFromBackendThread() {
        // Fixed issue #758
        return callFromMethods(BACKEND_THREAD);
    }

    private static boolean callFromEventHubNotify() {
        // Fixed issue #758
        // notify() will create thread when submit task to executor
        return callFromMethod("org.apache.hugegraph.event.EventHub", "notify");
    }

    private static boolean callFromAsyncTasks() {
        // Async tasks will create thread when submitted to executor
        return callFromMethods(ASYNC_TASKS);
    }

    private static boolean callFromBackendHbase() {
        // TODO: remove this unsafe entrance
        return callFromWorkerWithClass(HBASE_CLASSES);
    }

    private static boolean callFromSnapshot() {
        return callFromMethods(BACKEND_SNAPSHOT);
    }

    private static boolean callFromRaft() {
        return callFromWorkerWithClass(RAFT_CLASSES);
    }

    private static boolean callFromSofaRpc() {
        return callFromWorkerWithClass(SOFA_RPC_CLASSES);
    }

    private static boolean callFromNewSecurityException() {
        return callFromMethods(NEW_SECURITY_EXCEPTION);
    }

    private static boolean callFromIgnoreCheckedClass() {
        return callFromWorkerWithClass(IGNORE_CHECKED_CLASSES);
    }

    private static boolean callFromWorkerWithClass(Set<String> classes) {
        Thread curThread = Thread.currentThread();
        if (curThread.getName().startsWith(GREMLIN_SERVER_WORKER) ||
            curThread.getName().startsWith(TASK_WORKER)) {
            StackTraceElement[] elements = curThread.getStackTrace();
            for (StackTraceElement element : elements) {
                String className = element.getClassName();
                if (classes.contains(className)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean callFromMethods(Map<String, Set<String>> methods) {
        Thread curThread = Thread.currentThread();
        StackTraceElement[] elements = curThread.getStackTrace();
        for (StackTraceElement element : elements) {
            Set<String> clazzMethods = methods.get(element.getClassName());
            if (clazzMethods != null &&
                clazzMethods.contains(element.getMethodName())) {
                return true;
            }
        }
        return false;
    }

    private static boolean callFromMethod(String clazz, String method) {
        Thread curThread = Thread.currentThread();
        StackTraceElement[] elements = curThread.getStackTrace();
        for (StackTraceElement element : elements) {
            if (clazz.equals(element.getClassName()) &&
                method.equals(element.getMethodName())) {
                return true;
            }
        }
        return false;
    }
}
