/*
 * Copyright (C) 2018 Baidu, Inc. All Rights Reserved.
 */

package com.baidu.hugegraph.security;

import java.io.FileDescriptor;
import java.security.Permission;

import org.slf4j.Logger;

import com.baidu.hugegraph.util.Log;

public class HugeSecurityManager extends SecurityManager {

    private static final Logger LOG = Log.logger(HugeSecurityManager.class);

    private static final String GremlinExecutor_Class =
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
    public void checkAccess(ThreadGroup g) {
        if (this.callFromGremlin()) {
            throw new SecurityException(
                      "Not allowed to modify thread via gremlin");
        } else {
            super.checkAccess(g);
        }
    }

    @Override
    public void checkExit(int status) {
        if (this.callFromGremlin()) {
            throw new SecurityException(
                      "Not allowed to call System.exit() via gremlin");
        } else {
            super.checkExit(status);
        }
    }

    @Override
    public void checkRead(FileDescriptor fd) {
        if (this.callFromGremlin()) {
            throw new SecurityException(
                      "Not allowed to read file via gremlin");
        } else {
            super.checkRead(fd);
        }
    }

//    @Override
//    public void checkRead(String file) {
//        if (this.callFromGremlin()) {
//            throw new SecurityException("Not allowed to read file via gremlin");
//        } else {
//            super.checkRead(file);
//        }
//    }
//
//    @Override
//    public void checkRead(String file, Object context) {
//        if (this.callFromGremlin()) {
//            throw new SecurityException("Not allowed to read file via gremlin");
//        } else {
//            super.checkRead(file, context);
//        }
//    }

    @Override
    public void checkWrite(FileDescriptor fd) {
        if (this.callFromGremlin()) {
            throw new SecurityException(
                      "Not allowed to write file via gremlin");
        } else {
            super.checkWrite(fd);
        }
    }

    @Override
    public void checkWrite(String file) {
        if (this.callFromGremlin()) {
            throw new SecurityException(
                      "Not allowed to write file via gremlin");
        } else {
            super.checkWrite(file);
        }
    }

    @Override
    public void checkAccept(String host, int port) {
        if (this.callFromGremlin()) {
            throw new SecurityException(
                      "Not allowed to accept connect via gremlin");
        } else {
            super.checkAccept(host, port);
        }
    }

    @Override
    public void checkConnect(String host, int port) {
        if (this.callFromGremlin()) {
            throw new SecurityException(
                      "Not allowed to connect socket via gremlin");
        } else {
            super.checkConnect(host, port);
        }
    }

    private boolean callFromGremlin() {
        StackTraceElement elements[] = Thread.currentThread().getStackTrace();
        for (StackTraceElement element : elements) {
            String className = element.getClassName();
            if (GremlinExecutor_Class.equals(className)) {
                return true;
            }
        }
        return false;
    }
}
