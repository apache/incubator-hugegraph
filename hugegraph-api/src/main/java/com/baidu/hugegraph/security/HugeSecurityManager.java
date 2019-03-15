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

package com.baidu.hugegraph.security;

import java.io.FileDescriptor;
import java.security.Permission;

public class HugeSecurityManager extends SecurityManager {

    private static final String GREMLIN_EXECUTOR_CLASS =
            "org.apache.tinkerpop.gremlin.groovy.engine.ScriptEngines";

    @Override
    public void checkPermission(Permission permission) {
        // allow anything.
    }

    @Override
    public void checkPermission(Permission permission, Object context) {
        // allow anything.
    }

    @Override
    public void checkAccess(Thread t) {
        if (this.callFromGremlin()) {
            throw new SecurityException(
                      "Not allowed to modify thread via Gremlin");
        } else {
            super.checkAccess(t);
        }
    }

    @Override
    public void checkAccess(ThreadGroup g) {
        if (this.callFromGremlin()) {
            throw new SecurityException(
                      "Not allowed to modify thread group via Gremlin");
        } else {
            super.checkAccess(g);
        }
    }

    @Override
    public void checkExit(int status) {
        if (this.callFromGremlin()) {
            throw new SecurityException(
                      "Not allowed to call System.exit() via Gremlin");
        } else {
            super.checkExit(status);
        }
    }

    @Override
    public void checkRead(FileDescriptor fd) {
        if (this.callFromGremlin()) {
            throw new SecurityException(
                      "Not allowed to read file via Gremlin");
        } else {
            super.checkRead(fd);
        }
    }

    /**
     * TODO: Open these will lead server can't start, because it need read
     * conf files.
     * Need to achieve no check at startup, open check after startup is complete
     */
//    @Override
//    public void checkRead(String file) {
//        if (this.callFromGremlin()) {
//            throw new SecurityException("Not allowed to read file via Gremlin");
//        } else {
//            super.checkRead(file);
//        }
//    }
//
//    @Override
//    public void checkRead(String file, Object context) {
//        if (this.callFromGremlin()) {
//            throw new SecurityException("Not allowed to read file via Gremlin");
//        } else {
//            super.checkRead(file, context);
//        }
//    }

    @Override
    public void checkWrite(FileDescriptor fd) {
        if (this.callFromGremlin()) {
            throw new SecurityException(
                      "Not allowed to write file via Gremlin");
        } else {
            super.checkWrite(fd);
        }
    }

    @Override
    public void checkWrite(String file) {
        if (this.callFromGremlin()) {
            throw new SecurityException(
                      "Not allowed to write file via Gremlin");
        } else {
            super.checkWrite(file);
        }
    }

    @Override
    public void checkDelete(String file) {
        if (this.callFromGremlin()) {
            throw new SecurityException(
                      "Not allowed to delete file via Gremlin");
        } else {
            super.checkDelete(file);
        }
    }

    @Override
    public void checkAccept(String host, int port) {
        if (this.callFromGremlin()) {
            throw new SecurityException(
                      "Not allowed to accept connect via Gremlin");
        } else {
            super.checkAccept(host, port);
        }
    }

    @Override
    public void checkConnect(String host, int port) {
        if (this.callFromGremlin()) {
            throw new SecurityException(
                      "Not allowed to connect socket via Gremlin");
        } else {
            super.checkConnect(host, port);
        }
    }

    @Override
    public void checkConnect(String host, int port, Object context) {
        if (this.callFromGremlin()) {
            throw new SecurityException(
                      "Not allowed to connect socket via Gremlin");
        } else {
            super.checkConnect(host, port, context);
        }
    }

    private boolean callFromGremlin() {
        StackTraceElement[] elements = Thread.currentThread().getStackTrace();
        for (StackTraceElement element : elements) {
            String className = element.getClassName();
            if (GREMLIN_EXECUTOR_CLASS.equals(className)) {
                return true;
            }
        }
        return false;
    }
}
